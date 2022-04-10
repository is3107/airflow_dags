import requests
import pandas as pd
import datetime as dt
from datetime import date, datetime, timedelta
import csv
from bs4 import BeautifulSoup
import snscrape.modules.twitter as sntwitter
import re
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField
import pyarrow

def scrape_tweets(request):

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
    
    no_twitter_post = False

    start_epoch = ds
    end_epoch = start_epoch + timedelta(days=1)

    mapper = {
        'Sembcorp':'U96',
        'Ascendas':'A17U',
        'Capitaland':'C38U',
        'City Developments Limited':'C09',
        'Comfortdelgro':'C52',
        'Dairy Farm International Holdings':'D01',
        'DBS':'D05',
        'Frasers Centrepoint':'BUOU',
        'Genting Singapore':'G13',
        'Hongkong Land Holdings':'H78',
        'Jardine':'C07',
        'Jardine Matheson':'J36',
        'Keppel Corp':'BN4',
        'Keppel DC REIT':'AJBU',
        'Mapletree Commercial Trust':'N2IU',
        'Mapletree Industrial Trust':'ME8U',
        'Mapletree Logistics Trust':'M44U',
        'OCBC':'O39',
        'Sats Ltd':'S58',
        'SGX':'S68',
        'Singapore Airlines':'C6L',
        'Singtel':'Z74',
        'ST Engineering':'S63',
        'Thai Beverage PCL':'Y92',
        'UOB':'U11',
        'UOL':'U14',
        'Venture Corp':'V03',
        'Wilmar':'F34',
        'Yangzijiang Shipbuilding':'BS6',
    }

    
    start_date = start_epoch.strftime("%Y-%m-%d")
    end_date = end_epoch.strftime("%Y-%m-%d")

    maxTweets = 5

    created_utc = []
    tickers = []
    source = []
    tweets = []

    try:

        for search in mapper:
            for i,tweet in enumerate(sntwitter.TwitterSearchScraper(search + ' lang:en since:' + start_date + ' until:' + end_date + ' -filter:links -filter:replies').get_items()):
                if i > maxTweets:
                    break
                created_utc.append(tweet.date)
                tickers.append(mapper.get(search))
                source.append('Twitter seachword: ' + str(search))
                tweets.append(tweet.content)

        twitter = pd.DataFrame({'created_utc': created_utc, 
                        'ticker': tickers,
                        'source': source, 
                        'content': tweets})

        twitter['p_date'] = twitter['created_utc'].apply(lambda x: x.date())
        twitter = twitter.drop(columns=['created_utc'])

    except:
        no_twitter_post = True

    if no_twitter_post:
        return f"Successful run for date: {ds} "

    df = twitter
    df['content'] = df['content'].replace({r'(@[A-Za-z0–9_]+)|[^\x00-\x7F]|[^\w\s]||http\S+':''}, regex=True)
    df = df.replace(r'\n',' ', regex=True)
    df = df.drop_duplicates(subset=['content', 'ticker', 'p_date'], keep='first')
    df.sort_values(by='p_date', axis=0, ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = df[['p_date', 'ticker', 'source', 'content']]
    
    col_types = {'p_date': 'datetime64', 'ticker':'string', 'source':'string', 'content':'string'}
    df = df.astype(col_types)

    # send to BigQuery
    schema = [
        SchemaField('p_date', 'DATE', 'REQUIRED'),
        SchemaField('ticker', 'STRING', 'REQUIRED'),
        SchemaField('source', 'STRING', 'REQUIRED'),
        SchemaField('content', 'STRING', 'REQUIRED'),
    ]

    client = Client()
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    client.load_table_from_dataframe(df, destination=f"lti_ods.ods_ticker_level_twitter_posts${ds.strftime('%Y%m%d')}", job_config=job_config)

    return f"Successful run for date: {ds}"