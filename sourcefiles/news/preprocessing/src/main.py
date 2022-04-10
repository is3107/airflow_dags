import requests
import json
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
import re
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField, TableReference, ScalarQueryParameter, QueryJobConfig
import pyarrow
import os


root = os.path.dirname(os.path.abspath(__file__))
download_dir = os.path.join(root, 'sentiment')
#os.chdir(download_dir)
nltk.data.path.append(download_dir)


bqclient = Client()


def preprocessing(request):

    request_json = request.get_json(silent=True)
    request_args = request.args
    request_data = request.data

    if request_json and 'ds' in request_json:
        ds = request_json['ds']
    elif request_args and 'ds' in request_args:
        ds = request_args['ds']
    elif request_data:
        ds = json.loads(request_data)['ds']

    ds = datetime.strptime(ds, "%Y-%m-%d").date()
    no_data_for_today = False

    query_string = """
    SELECT p_date, ticker, title, source, description, url
    FROM `is3107.lti_ods.ods_ticker_level_financial_news_scraping_results`
    WHERE p_date = @ds
    """

    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("ds", "DATE", ds),   
        ]
    )

    df = (
        bqclient.query(query_string, job_config=job_config)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )

    try:
        def analyser(row):
            sia = SentimentIntensityAnalyzer()
            score_dict = sia.polarity_scores(row['description'])
            return score_dict['compound']


        df['NLTK_score'] = df.apply(analyser, axis=1)

        def sentiment_mapper(row):
            if row['NLTK_score'] > 0.4:
                return 'positive'
            elif row['NLTK_score'] < -0.4:
                return 'negative'
            else:
                return 'neutral'

        df['NLTK_sentiment'] = df.apply(sentiment_mapper, axis=1)

    except:
        no_data_for_today = True

    if no_data_for_today:
        return f"Successful run for date: {ds}"

    col_types = {'p_date': 'datetime64', 'ticker': 'string', 'title': 'string', 'source': 'string', 'description': 'string', 'url':'string',
    'NLTK_score':'float', 'NLTK_sentiment':'string'}

    df = df.astype(col_types)

    # send to BigQuery
    schema = [
        SchemaField('p_date', 'DATE', 'REQUIRED'),
        SchemaField('ticker', 'STRING', 'REQUIRED'),
        SchemaField('title', 'STRING', 'REQUIRED'),
        SchemaField('source', 'STRING', 'REQUIRED'),
        SchemaField('description', 'STRING', 'REQUIRED'),
        SchemaField('url', 'STRING', 'REQUIRED'),
        SchemaField('NLTK_score', 'FLOAT', 'REQUIRED'),
        SchemaField('NLTK_sentiment', 'STRING', 'REQUIRED')
    ]

    client = Client()
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)

    ## change bucket name and the partitioning 
    client.load_table_from_dataframe(df, destination=f"lti_dim.dim_financial_news_with_nltk${ds.strftime('%Y%m%d')}", job_config=job_config)

    return f"Successful run for date: {ds}"


    
    