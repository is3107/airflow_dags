from urllib.error import HTTPError
from airflow.providers.google.cloud.operators.functions import CloudFunctionDeployFunctionOperator
from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.hooks.functions import CloudFunctionsHook
from google.cloud import storage
from googleapiclient.errors import HttpError
from templates.paths import Paths
import google.auth.transport.requests
import google.oauth2.id_token
import hashlib
import base64
from templates import template
import shutil
import logging
from os.path import exists, join
from os import remove, makedirs

"""
This is a custom operator that combines the 4 operators used for the ingestion pipeline

Arguments:
    source_path <str> : location of the python file that pulls the data, usually taken from Paths class
    deploy_body <dict> : dict that specifies the 'body' parameter for deployment on Google Functions, usually taken from Body class
    date <date-like>: date string or date for execution. Meant to take in 'ds' from the DAG

The workflow is as such:
    generating requirements.txt > zipping source file and requirements.txt >
    checking if source file differs from currently deploy source file for the same function >
    deploy function if source file differs > invoking Cloud Function

"""
class IngestionOperator(BaseOperator):
    # define date as a template field so constructor can take in DAG template variable 'ds'
    template_fields = ["date"]

    def __init__(self, source_path:str, deploy_body: dict, date, **kwargs) -> None:
        super().__init__(**kwargs)
        self.path = source_path
        self.body = deploy_body
        self.date = date
        self.invoke_timeout = deploy_body.get("timeout", "60s")

    def execute(self, context):
        logging.info(f"Source Path is at: {self.path}")
        logging.info(f"Deploy Body is: {self.body}")
        logging.info(f"Partition date is: {self.date}")

        # check for blocking file
        path_to_blocking_file = join(self.path, "tmp", ".block")

        if not exists(path_to_blocking_file):
            try:
                # create blocking file
                if not exists(join(self.path, "tmp")):
                    logging.info("Creating tmp subdirectory")
                    makedirs(join(self.path, "tmp"))
                    
                logging.info(f"Blocking file not detected, creating blocking file in directory: {path_to_blocking_file}")
                open(path_to_blocking_file, 'a').close()

                # Check folder structure
                if exists(join(self.path, "main.py")):
                    logging.error("Source files should be in subdirectory /src")
                    assert not exists(join(self.path, "main.py"))

                # generate requirements using template
                logging.info("Generating requirements.txt")
                template.generate_requirements(task_id = 'generate_requirements', path=self.path, date=self.date).execute(context)


                # zip files
                logging.info("Zipping source files")
                src_path = join(self.path, "src")
                output_path = join(self.path, self.path.split("/")[-1])
                shutil.make_archive(base_name=output_path, format='zip', root_dir=src_path)

                # get hash of cloud function source file
                logging.info("Getting hash of source file on GCP")
                has_match = False
                try:
                    cloud_function = CloudFunctionsHook("v1").get_function(self.body['name'])
                    function_version = cloud_function["versionId"]
                except HttpError:
                    logging.info("Function has not been deployed before")
                    function_version = -1

                if function_version != -1:
                    function_name = self.body['name'].split('/')[-1]

                    client = storage.Client()
                    bucket = client.get_bucket(template.GCP_FUNCTION_BUCKET)
                    all_blobs = list(client.list_blobs(bucket))
                    

                    for blob in all_blobs:
                        if function_name == blob.name.split('-')[0] and function_version == blob.name.split('/')[1].split('-')[1]:
                            gcp_md5 = blob.md5_hash
                            logging.info(f"GCP MD5 Hash is: {gcp_md5}")
                            with open(output_path + ".zip", "rb") as source_file:
                                source_file_contents = source_file.read()
                            current_md5 = base64.b64encode(hashlib.md5(source_file_contents).digest()).decode()
                            logging.info(f"Local MD5 Hash is: {current_md5}")

                            if gcp_md5 == current_md5:
                                logging.info("The hashes match, skipping deployment")
                                has_match = True
                                break


                # deploy cloud function 
                # only happens when the source file and deployed file are different            
                if (not has_match):
                    logging.info("Hashes do not match, deploying function to GCP")
                    CloudFunctionDeployFunctionOperator(
                        task_id = 'deploy_cloud_function',
                        body = self.body,
                        validate_body = True,
                        location = 'us-west1',
                        zip_path = output_path + '.zip'
                    ).execute(context)
            finally:
                # Remove blocking file
                remove(path_to_blocking_file)
        else:
            logging.info("Blocking file detected, skipping deploy steps")

        # Wait for first concurrent run to finish deploying
        while exists(path_to_blocking_file):
            pass

        # Get Authorization Token
        audience = Paths.GCP_FUNCTIONS_BASE_URL + self.body['name'].split('/')[-1]
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)


        # invoke cloud function
        timeout_in_float = float(self.invoke_timeout[:-1])
        logging.info("Invoking cloud function")
        SimpleHttpOperator(
            http_conn_id = 'gcp_functions',
            task_id='invoke_function',
            method='POST',
            endpoint=self.body['name'].split('/')[-1],
            data=f'{{"ds": "{self.date}"}}',
            headers={
                "Authorization": f"Bearer {id_token}",
                "Content-Type": "application/json"
            },
            extra_options={"timeout": timeout_in_float}
        ).execute(context)

        return "Ingestion Job Successful"
