from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.decorators import dag
import pendulum

from templates.paths import Paths
from templates.body import Body
from templates.IngestionOperator import IngestionOperator

from datetime import timedelta

default_args={
    'owner': 'Kai Herng',
    'retries': 3
}

# adding variables to feed into the source py file
@dag(
    default_args=default_args,
    description='Internal Analysts Daily Transactions',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 20, tz='Asia/Singapore'),
    catchup=False,
    tags=['internal', 'ods', 'dwd'],
    template_searchpath='/home/is3107/airflow/dags/templates/sql'
)

def ingestion_internal2bq_internal_analyst_txn():

    check_upstream = ExternalTaskSensor(
        task_id='check_for_upstream',
        external_dag_id='ingestion_yfinancesgx2bq_stock_prices',
        external_task_id='dwd_stock_prices_daily',
        failed_states=['failed', 'skipped']
    )

    ods_internal_analyst_txn_daily = IngestionOperator(
        source_path = Paths.SIMULATE_INTERNAL_TXN, 
        deploy_body = Body.SIMULATE_INTERNAL_TXN,
        task_id = 'ods_internal_analyst_txn_daily',
        date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    )

    check_yesterday = ExternalTaskSensor(
        task_id='check_for_yesterday_portfolio',
        external_dag_id='analytics_internal_analyst_portfolio_performance',
        external_task_id='dwa_all_portfolio_snapshot_daily',
        execution_delta=timedelta(days=1),
        failed_states=['failed', 'skipped']
    )

    dwd_internal_analyst_txn_daily = BigQueryInsertJobOperator(
        task_id="dwd_internal_analyst_txn_daily",
        configuration={
            "query": {
                "query": "{% include 'dwd_internal_analyst_txn_daily.sql' %}",
                "destinationTable": {
                    "projectId": "is3107",
                    "datasetId": "lti_dwd",
                    "tableId": "dwd_internal_analyst_txn_daily${{ dag_run.logical_date.astimezone(dag.timezone) | ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False
            }
        }
    )
    
    check_upstream >> ods_internal_analyst_txn_daily >> check_yesterday >> dwd_internal_analyst_txn_daily

simulate_internal_txn = ingestion_internal2bq_internal_analyst_txn()







