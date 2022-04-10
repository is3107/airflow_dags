from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.decorators import dag
import pendulum


default_args={
    'owner': 'Timothy',
    'retries': 3
}

# adding variables to feed into the source py file
@dag(
    default_args=default_args,
    description='Aggregation of Stock Prices and Financial Data for DM',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 20, tz='Asia/Singapore'),
    catchup=False,
    tags=['prices', 'financials', 'dm'],
    template_searchpath='/home/is3107/airflow/dags/templates/sql'
)

def analytics_stock_prices_and_financials():

    check_upstream_prices = ExternalTaskSensor(
        task_id='check_for_upstream_prices',
        external_dag_id='ingestion_yfinancesgx2bq_stock_prices',
        external_task_id='dwd_stock_prices_daily',
        failed_states=['failed', 'skipped']
    )

    check_upstream_financials = ExternalTaskSensor(
        task_id='check_for_upstream_financials',
        external_dag_id='ingestion_yfinance2bq_stock_financials',
        external_task_id='dwd_yfinance_financials',
        failed_states=['failed', 'skipped']
    )

    dm_stock_prices_and_financials = BigQueryInsertJobOperator(
        task_id="dm_stock_prices_and_financials_daily",
        configuration={
            "query": {
                "query": "{% include 'dm_stock_prices_and_financials_daily.sql' %}",
                "destinationTable": {
                    "projectId": "is3107",
                    "datasetId": "lti_dm",
                    "tableId": "dm_stock_prices_and_financials_daily${{ dag_run.logical_date.astimezone(dag.timezone) | ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False
            }
        }
    )
    
    [check_upstream_prices, check_upstream_financials] >> dm_stock_prices_and_financials

dm_stock_prices_and_financials = analytics_stock_prices_and_financials()