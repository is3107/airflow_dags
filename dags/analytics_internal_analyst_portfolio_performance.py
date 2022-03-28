from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.decorators import dag, task, task_group
import pendulum

from templates import template
from templates.paths import Paths
from templates.body import Body
import logging

default_args = {
    'owner': 'Darren',
    'retries': 3
}

doc_md = """
# Internal Analysts' Portfolio Performance

Provides performance data on the various stock portfolios that the company's internal analysts have been managing.

## Tables involved:



"""

@dag(
    default_args=default_args,
    description='Internal Analysts\' Portfolio Performance',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 3, 20, tz='Asia/Singapore'),
    catchup=False,
    tags=['internal', 'dwa', 'dm']
)

def analytics_internal_analyst_portfolio_performance():
    ## generate dwa_all_portfolio_snapshot_daily
    return

analytics_internal_analyst_portfolio = analytics_internal_analyst_portfolio_performance()