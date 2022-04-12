from airflow.decorators import dag
import pendulum

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args={
    'owner': 'verren pramita',
    'retries': 0
}

@dag(
    default_args=default_args,
    description='sentiments pipeline to dim, dwa and dm',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 25, tz='Asia/Singapore'),
    catchup=False,
    tags=['sentiments', 'dwa', 'dm'],
    template_searchpath='/home/is3107/airflow/dags/templates/sql'
)


def analytics_sentiments():

    check_upstream = ExternalTaskSensor(
        task_id='check_for_upstream',
        external_dag_id='ingestion_socialmedia2bq_sentiments',
        failed_states=['failed']
    )

    check_upstream_stock_code = ExternalTaskSensor(
        task_id='check_for_upstream_stock_code',
        external_dag_id='ingestion_sgx2bq_stock_code_list',
        failed_states=['failed']
    )
    
    aggregation = BigQueryInsertJobOperator(
        task_id="dwa_aggregate_sentiments",
        configuration={
            "query": {
                "query": "{% include 'dwa_sentiments_daily.sql' %}",
                "destinationTable": {
                    "projectId": "is3107",
                    "datasetId": "lti_dwa",
                    "tableId": "dwa_aggregated_daily_sentiments_data${{ dag_run.logical_date.astimezone(dag.timezone) | ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False
            }
        }
    )

    export_to_dm = BigQueryInsertJobOperator(
        task_id="dm_export_sentiments",
        configuration={
            "query": {
                "query": "{% include 'dm_sentiments_daily.sql' %}",
                "destinationTable": {
                    "projectId": "is3107",
                    "datasetId": "lti_dm",
                    "tableId": "dm_aggregated_sentiments_daily${{ dag_run.logical_date.astimezone(dag.timezone) | ds_nodash }}"
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False
            }
        }
    )

    [check_upstream, check_upstream_stock_code] >> aggregation >> export_to_dm

dag = analytics_sentiments()