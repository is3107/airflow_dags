from airflow.operators.bash_operator import BashOperator
import shutil

from templates.paths import Paths

"""
Default body parameter for deploying Cloud Function
addtional_args is a dict to pass/overwrite additional details to the body
"""
def default_body(additional_args = None):
    body =  {
    "name": "default",
    "description": "default description here",
    "entryPoint": "entry_point",
    "availableMemoryMb": 256,
    "httpsTrigger": {},
    "runtime": "python39",
    "sourceUploadUrl": "" }
    if additional_args:
        body.update(additional_args)
    return body 

"""
Uses BashOperator to call pipreqs CLI
requires an absolute path, not relative path 
"""
def generate_requirements(task_id:str, path:str):
    bash_command = f"{Paths.GENERATE_REQ_SCRIPT} {path}"
    return BashOperator(task_id = task_id, bash_command = bash_command)


"""
Zips sourcefile for upload to Google Cloud Functions
requires an absolute path, not relative path 
DO NOT USE - Kai Herng 
"""
def zip_directory(path:str):
    shutil.make_archive(base_name=path, format='zip', root_dir=path)
    return 

"""
Bucket name where GCP Cloud Functions are stored
"""
GCP_FUNCTION_BUCKET = "gcf-sources-118045634760-us-west1"
