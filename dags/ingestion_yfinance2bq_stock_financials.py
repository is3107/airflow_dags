from airflow.decorators import dag
import pendulum

from templates.paths import Paths
from templates.body import Body

from templates.IngestionOperator import IngestionOperator

# dag config
default_args={
    'owner': 'Timothy',
    'retries': 3
}

@dag(
    default_args=default_args,
    description='yfinance Financials Cloud Pipeline',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 23, tz='Asia/Singapore'),
    catchup=False,
    tags=['yfinance', 'ods', 'dwd'],
)

# main python ingestion function
def ingestion_yfinance2bq_stock_financials():

    ingestion = IngestionOperator(
        source_path = Paths.YFINANCE_STOCK_FINANCIALS, 
        deploy_body = Body.YFINANCE_STOCK_FINANCIALS,
        task_id = 'ingest_yfinance_financials',
        date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    )    

    dwd = IngestionOperator(
        source_path = Paths.YFINANCE_STOCK_FINANCIALS_DWD,
        deploy_body = Body.YFINANCE_STOCK_FINANCIALS_DWD,
        task_id = 'dwd_yfinance_financials',
        date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    )
    
    ingestion >> dwd

dag_yfinance_financials = ingestion_yfinance2bq_stock_financials()