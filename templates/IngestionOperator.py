from urllib.error import HTTPError
from airflow.providers.google.cloud.operators.functions \
     import CloudFunctionInvokeFunctionOperator, CloudFunctionDeployFunctionOperator
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.functions import CloudFunctionsHook
from google.cloud import storage
from googleapiclient.errors import HttpError
import hashlib
import base64
from templates import template
import shutil
import logging


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

    def execute(self, context):
        logging.info(f"Source Path is at: {self.path}")
        logging.info(f"Deploy Body is: {self.body}")
        logging.info(f"Partition date is: {self.date}")

        # generate requirements using template
        logging.info("Generating requirements.txt")
        template.generate_requirements(task_id = 'generate_requirements', path =self.path).execute(context)


        # zip files
        logging.info("Zipping source files")
        shutil.make_archive(base_name=self.path, format='zip', root_dir=self.path)

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
                    with open(self.path + ".zip", "rb") as source_file:
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
                zip_path = self.path + '.zip'
            ).execute(context)


        # invoke cloud function
        logging.info("Invoking cloud function")
        CloudFunctionInvokeFunctionOperator(
            task_id = 'invoke_cloud_function',
            function_id = self.body['name'].split('/')[-1],  
            input_data =  {"data": f'{{"ds": "{self.date}"}}'},  
            location = 'us-west1'
        ).execute(context)

        return "Ingestion Job Successful"
