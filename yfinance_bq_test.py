from airflow.decorators import dag, task
import pendulum
import logging


default_args={
    'owner': 'is3107',
    'retries': 5
}

@dag(
    default_args=default_args,
    description='Test Data yfinance pipeline',
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 3, 1, tz="UTC"),
    catchup=False,
    tags=['yfinance'],
)

def yfinance_bq_test_etl():

    @task
    def yfinance2bq_apple_daily(ds=None):
        import yfinance as yf
        from google.cloud import bigquery
        
        # Download data
        logging.info("Downloading Data")
        data=yf.download("AAPL", start='2022-03-01', end='2022-03-05')
        logging.info("Download Completed")

        # Fix indexes
        logging.info("Fixing data indexes and adding p_date column")
        data.reset_index(inplace=True)
        data.columns=data.columns.str.lower()
        data.columns=data.columns.str.replace(' ', '_')

        # Add partition date column
        data['p_date']=ds

        # Remove old partition data from BigQuery
        logging.info("Deleting old data from partition")
        client=bigquery.Client()
        query=f"DELETE is3107.ods.ods_yfinance_apple_daily WHERE p_date = \"{ds}\""
        query_job=client.query(query)
        query_job.result()

        # Load new data to BigQuery
        logging.info("Loading new data into partition")
        data.to_gbq('ods.ods_yfinance_apple_daily', if_exists='append', table_schema=[
            {
                'name': 'date',
                'type': 'DATETIME'
            },
            {
                'name': 'p_date',
                'type': 'DATE'
            }
        ])
        

    yfinance2bq = yfinance2bq_apple_daily()

yfinance_bq_test_etl_dag = yfinance_bq_test_etl()