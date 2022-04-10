import requests
import pandas as pd
import datetime as dt
from datetime import date, datetime, timedelta
from psaw import PushshiftAPI
import csv
from bs4 import BeautifulSoup
import snscrape.modules.twitter as sntwitter
import re
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField
import pyarrow

def scrape_text_data(request):

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
    
    no_reddit_post = False
    no_twitter_post = False

    api = PushshiftAPI()

    start_epoch = ds
    end_epoch = start_epoch + timedelta(days=1)

    subreddit_list = ['SGXBets',
                      'investingsingapore',
                      'SingaporeStocks',
                      'SGX']

    created_utc = []
    source = []
    post_title = []

    try:
        for sub in subreddit_list:
            lst = list(api.search_submissions(after=start_epoch,
                                         before=end_epoch,
                                        subreddit=sub,
                      filter=['url','author', 'title', 'subreddit'], limit=3000000))

            for submission in lst:
                created_utc.append(submission.created_utc)
                source.append('Reddit: ' + submission.subreddit)
                post_title.append(submission.title)

        reddit = pd.DataFrame({'created_utc': created_utc, 
                           'source': source, 
                           'title': post_title})

        reddit['created_utc'] = pd.to_datetime(reddit['created_utc'], unit='s')
        reddit['date'] = reddit['created_utc'].apply(lambda x: x.date())
        reddit = reddit.drop(columns=['created_utc'])

    except:
        no_reddit_post = True
        print("No reddit posts for today: " + str(dt.datetime.today().strftime("%Y-%m-%d")))

    hashtags = ['SGX', 'SGXstocks', 'SingaporeStocks', 'SGXNIFTY']
    searches = ["Singapore Exchange"]
    
    start_date = start_epoch.strftime("%Y-%m-%d")
    end_date = end_epoch.strftime("%Y-%m-%d")

    created_utc = []
    source = []
    post_title = []

    try:

        for hashtag in hashtags:
            for i,tweet in enumerate(sntwitter.TwitterHashtagScraper(hashtag + ' lang:en since:' + start_date + ' until:' + end_date + ' -filter:links -filter:replies').get_items()):
                    created_utc.append(tweet.date)
                    source.append('Twitter: #SGX')
                    post_title.append(tweet.content)

        for search in searches:
            for i,tweet in enumerate(sntwitter.TwitterSearchScraper(search + ' lang:en since:' + start_date + ' until:' + end_date + ' -filter:links -filter:replies').get_items()):
                    created_utc.append(tweet.date)
                    source.append('Twitter seachword: SGX')
                    post_title.append(tweet.content)

        twitter = pd.DataFrame({'created_utc': created_utc, 
                        'source': source, 
                        'title': post_title})

        twitter['date'] = twitter['created_utc'].apply(lambda x: x.date())
        twitter = twitter.drop(columns=['created_utc'])
    
    except:
        no_twitter_post = True
        print("No twitter posts for today: " + str(dt.datetime.today().strftime("%Y-%m-%d")))

    if no_reddit_post and no_twitter_post:
        return "no reddit and twitter post for today"
    elif no_reddit_post:
        df = twitter
    elif no_twitter_post:
        df = reddit
    else:
        df = pd.concat([reddit,twitter],ignore_index=True)

    df['title'] = df['title'].replace({r'(@[A-Za-z0–9_]+)|[^\x00-\x7F]|[^\w\s]||http\S+':''}, regex=True)
    df = df.drop_duplicates(subset=['title', 'date'], keep='first')
    df.sort_values(by='date', axis=0, ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    df = df.rename(columns={'date': 'p_date'})
    
    col_types = {'source': 'string', 'title': 'string', 'p_date': 'datetime64'}
    df = df.astype(col_types)

    # send to BigQuery
    schema = [
        SchemaField('source', 'STRING', 'REQUIRED'),
        SchemaField('title', 'STRING', 'REQUIRED'),
        SchemaField('p_date', 'DATE', 'REQUIRED')
    ]

    client = Client()
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    client.load_table_from_dataframe(df, destination=f"lti_ods.ods_reddit_scraping_results_raw${ds.strftime('%Y%m%d')}", job_config=job_config)

    return f"Successful run for date: {ds}"