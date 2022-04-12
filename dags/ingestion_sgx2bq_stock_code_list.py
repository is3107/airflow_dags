from airflow.decorators import dag
import pendulum

from templates.paths import Paths
from templates.body import Body

from templates.IngestionOperator import IngestionOperator

default_args={
    'owner': 'Matheus Aaron',
    'retries': 2
}

# adding variables to feed into the source py file
@dag(
    default_args=default_args,
    description='Collect Stock Codes Listed on SGX',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 25, tz='Asia/Singapore'),
    catchup=False,
    tags=['sgx', 'dwd'],
)


def ingestion_sgx2bq_stock_code_list():
    
    sgx_stock_code_ingestion = IngestionOperator(
        source_path = Paths.SGX_STOCK_CODE_SCRAPING, 
        deploy_body = Body.SGX_STOCK_CODE_SCRAPING,
        task_id = 'sgx_stock_code_ingestion',
        date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    )

    sgx_stock_code_ingestion

sgx_stock_code_scraping_dag = ingestion_sgx2bq_stock_code_list()
