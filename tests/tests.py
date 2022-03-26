import logging
import os
import sys

import pytest
import re
from airflow.models import DagBag

sys.path.append(os.path.join(os.path.dirname(__file__), "../dags"))
ingestion_pattern = r"ingestion_[a-zA-Z]*2[a-zA-Z]*_\w*"
analytics_pattern = r"analytics_\w*"

@pytest.fixture(params=["../dags"])
def dag_bag(request):
    return DagBag(dag_folder=request.param, include_examples=False)

def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors
    
def test_dag_file_names_matches_pattern():
    logging.info(f"Checking that DAG ID follows either the ingestion pattern: {ingestion_pattern}.py or the analytifcs pattern: {analytics_pattern}.py")
    wrong_names = []
    (_, _, filenames) = next(os.walk(os.path.join(os.path.dirname(__file__), "../dags")))
    for file_name in filenames:
        if not re.fullmatch(ingestion_pattern + '.py', file_name) and not re.fullmatch(analytics_pattern + '.py', file_name):
            wrong_names.append(file_name)

    for names in wrong_names:
        logging.warning(f"The DAG with DAG ID: {names} does not match the name formats")

    assert len(wrong_names) == 0

def test_dag_id_matches_pattern(dag_bag):
    wrong_names = []
    logging.info(f"Checking that DAG ID follows either the ingestion pattern: {ingestion_pattern} or the analytifcs pattern: {analytics_pattern}")
    for dag_id, dag in dag_bag.dags.items():
        if not re.fullmatch(ingestion_pattern, dag_id) and not re.fullmatch(analytics_pattern, dag_id):
            wrong_names.append(dag_id)

    for names in wrong_names:
        logging.warning(f"The DAG with DAG ID: {names} does not match the name formats")

    assert len(wrong_names) == 0