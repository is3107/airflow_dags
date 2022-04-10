from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.functions \
     import CloudFunctionInvokeFunctionOperator, CloudFunctionDeployFunctionOperator
from airflow.decorators import dag
import pendulum
from templates import template
from templates.paths import Paths
from templates.body import Body
import logging
from templates.IngestionOperator import IngestionOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args={
    'owner': 'verren pramita',
    'retries': 0
}

@dag(
    default_args=default_args,
    description='sentiments ingestion of raw data',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 25, tz='Asia/Singapore'),
    catchup=False,
    tags=['sentiments', 'ods', 'dim']
)


def ingestion_socialmedia2bq_sentiments():

    with TaskGroup(group_id='ingestion_sentiments') as ingestion_task_group:
    
        reddit_ingestion = IngestionOperator(
            source_path = Paths.REDDIT_SCRAPING, 
            deploy_body = Body.REDDIT_SCRAPING,
            task_id = 'reddit_ingestion',
            date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
        )

        twitter_ingestion = IngestionOperator(
            source_path = Paths.TWITTER_SCRAPING, 
            deploy_body = Body.TWITTER_SCRAPING,
            task_id = 'twitter_ingestion',
            date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
        )

        news_ingestion = IngestionOperator(
            source_path = Paths.NEWS_SCRAPING, 
            deploy_body = Body.NEWS_SCRAPING,
            task_id = 'news_ingestion',
            date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
        )

        [reddit_ingestion, twitter_ingestion, news_ingestion]


    with TaskGroup(group_id='preprocessing_sentiments') as preprocessing_task_group:

        reddit_preprocessing = IngestionOperator(
            source_path = Paths.REDDIT_PREPROCESSING,
            deploy_body = Body.REDDIT_PREPROCESSING,
            task_id = 'reddit_preprocessing',
            date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
        )


        twitter_preprocessing = IngestionOperator(
            source_path = Paths.TWITTER_PREPROCESSING,
            deploy_body = Body.TWITTER_PREPROCESSING,
            task_id = 'twitter_preprocessing',
            date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
        )


        news_preprocessing = IngestionOperator(
            source_path = Paths.NEWS_PREPROCESSING,
            deploy_body = Body.NEWS_PREPROCESSING,
            task_id = 'news_preprocessing',
            date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
        )

        [reddit_preprocessing, twitter_preprocessing, news_preprocessing]
        

    ingestion_task_group >> preprocessing_task_group


dag = ingestion_socialmedia2bq_sentiments()