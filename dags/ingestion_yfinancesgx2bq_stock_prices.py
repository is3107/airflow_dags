from airflow.decorators import dag
import pendulum

from templates.paths import Paths
from templates.body import Body


from templates.IngestionOperator import IngestionOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# dag config
default_args={
    'owner': 'Timothy',
    'retries': 3
}

@dag(
    default_args=default_args,
    description='yfinance Prices Cloud Pipeline',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 23, tz='Asia/Singapore'),
    catchup=False,
    tags=['yfinance', 'ods', 'dwd'],
    template_searchpath='/home/is3107/airflow/dags/templates/sql'
)

# main python ingestion function
def ingestion_yfinancesgx2bq_stock_prices():

    ingestion_sgx = IngestionOperator(
        source_path = Paths.SGX_STOCK_PRICES, 
        deploy_body = Body.SGX_STOCK_PRICES, 
        task_id = 'ods_sgx_stock_price_daily',
        date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    )

    ingestion_forex = IngestionOperator(
        source_path = Paths.FOREX_TO_SGD, 
        deploy_body = Body.FOREX_TO_SGD, 
        task_id = 'dwd_exchange_rates_daily',
        date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    )

    ingestion_yfinance = IngestionOperator(
        source_path = Paths.YFINANCE_STOCK_PRICES, 
        deploy_body = Body.YFINANCE_STOCK_PRICES, 
        task_id = 'ods_yfinance_stock_prices_daily',
        date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    )    

    dwd = BigQueryInsertJobOperator(
        task_id="dwd_stock_prices_daily",
        configuration={
            "query": {
                "query": "{% include 'dwd_stock_prices_daily.sql' %}",
                "destinationTable": {
                    "projectId": "is3107",
                    "datasetId": "lti_dwd",
                    "tableId": "dwd_stock_prices_daily${{ dag_run.logical_date.astimezone(dag.timezone) | ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            }
        }
    )
    
    ingestion_sgx >> [ingestion_forex, ingestion_yfinance] >> dwd

dag_yfinance_prices = ingestion_yfinancesgx2bq_stock_prices()