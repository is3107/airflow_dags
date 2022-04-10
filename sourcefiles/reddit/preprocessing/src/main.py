import requests
import json
import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from psaw import PushshiftAPI
import csv
from bs4 import BeautifulSoup
import snscrape.modules.twitter as sntwitter
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

    # https://cloud.google.com/bigquery/docs/samples/bigquery-query-params-named#bigquery_query_params_named-python

    query_string = """
    SELECT source, title, p_date
    FROM `is3107.lti_ods.ods_reddit_scraping_results_raw`
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
            score_dict = sia.polarity_scores(row['title'])
            return score_dict['compound']

        df['NLTK_SIA_compound_score'] = df.apply(analyser, axis=1)

        def sentiment_mapper(row):
            if row['NLTK_SIA_compound_score'] > 0.4:
                return 'positive'
            elif row['NLTK_SIA_compound_score'] < -0.4:
                return 'negative'
            else:
                return 'neutral'

        df['NLTK_sentiment'] = df.apply(sentiment_mapper, axis=1)
        df['p_date'] = pd.to_datetime(df['p_date'])
    
    except:
        no_data_for_today = True

    if no_data_for_today:
        return f"Successful run for date: {ds}"

    col_types = {'source': 'string', 'title': 'string', 'p_date': 'datetime64', 'NLTK_SIA_compound_score':'float', 'NLTK_sentiment':'string'}
    df = df.astype(col_types)
    
    # send to BigQuery
    schema = [ 
        SchemaField('source', 'STRING', 'REQUIRED'), 
        SchemaField('title', 'STRING', 'REQUIRED'),
        SchemaField('p_date', 'DATE', 'REQUIRED'),
        SchemaField('NLTK_SIA_compound_score', 'FLOAT', 'REQUIRED'),
        SchemaField('NLTK_sentiment', 'STRING', 'REQUIRED')
    ]

    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    bqclient.load_table_from_dataframe(df, destination=f"lti_dim.dim_reddit_with_nltk${ds.strftime('%Y%m%d')}", job_config=job_config)

    
    return f"Successful for date {ds}"