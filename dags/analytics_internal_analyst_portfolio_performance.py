from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.decorators import dag
import pendulum

default_args = {
    'owner': 'Darren',
    'retries': 3
}

doc_md = """
# Internal Analysts' Portfolio Performance

Provides performance data on the various stock portfolios that the company's internal analysts have been managing.

Every portfolio is tied to an analyst.

"""

@dag(
    default_args=default_args,
    description='Internal Analysts\' Portfolio Performance',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 20, tz='Asia/Singapore'),
    catchup=False,
    tags=['internal', 'dwa', 'dm'],
    template_searchpath='/home/is3107/airflow/dags/templates/sql',
    doc_md=doc_md
)

def analytics_internal_analyst_portfolio_performance():
    check_upstream = ExternalTaskSensor(
        task_id='check_for_upstream',
        external_dag_id='ingestion_internal2bq_internal_analyst_txn',
        external_task_id='dwd_internal_analyst_txn_daily',
        failed_states=['failed', 'skipped']
    )

    dwa_all_portfolio_snapshot_daily = BigQueryInsertJobOperator(
        task_id="dwa_all_portfolio_snapshot_daily",
        configuration={
            "query": {
                "query": "{% include 'dwa_all_portfolio_snapshot_daily.sql' %}",
                "destinationTable": {
                    "projectId": "is3107",
                    "datasetId": "lti_dwa",
                    "tableId": "dwa_all_portfolio_snapshot_daily${{ dag_run.logical_date.astimezone(dag.timezone) | ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False
            }
        }
    )

    dm_all_portfolio_snapshot_daily = BigQueryInsertJobOperator(
        task_id="dm_all_portfolio_snapshot_daily",
        configuration={
            "query": {
                "query": "{% include 'dm_all_portfolio_snapshot_daily.sql' %}",
                "destinationTable": {
                    "projectId": "is3107",
                    "datasetId": "lti_dm",
                    "tableId": "dm_all_portfolio_snapshot_daily${{ dag_run.logical_date.astimezone(dag.timezone) | ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False
            }
        }
    )

    check_upstream >> dwa_all_portfolio_snapshot_daily >> dm_all_portfolio_snapshot_daily

analytics_internal_analyst_portfolio = analytics_internal_analyst_portfolio_performance()