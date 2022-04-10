import pandas as pd
import requests
from bs4 import BeautifulSoup
import datetime as dt
from datetime import date, datetime, timedelta
import re
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField
import pyarrow

def scrape_news(request):

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


    def scrape():

        tickers = []
        date = []
        title = []
        desc = []
        source = []
        url = []

        HEADERS = {
                "User-Agent": "Mozilla/5.0",
        }
        
        mapper = {
            'U96':'https://sginvestors.io/sgx/stock/u96-sembcorp-ind/news-article',
            'A17U':'https://sginvestors.io/sgx/reit/a17u-ascendas-reit/news-article',
            'C38U': 'https://sginvestors.io/sgx/reit/c38u-capland-intcom-t/news-article',
            'C09': 'https://sginvestors.io/sgx/stock/c09-citydev/news-article',
            'C52': 'https://sginvestors.io/sgx/stock/c52-comfortdelgro/news-article',
            'D01': 'https://sginvestors.io/sgx/stock/d01-dairyfarm-usd/news-article',
            'D05': 'https://sginvestors.io/sgx/stock/d05-dbs/news-article',
            'BUOU': 'https://sginvestors.io/sgx/reit/buou-frasers-lnc-tr/news-article',
            'G13': 'https://sginvestors.io/sgx/stock/g13-genting-sing/news-article',
            'H78': 'https://sginvestors.io/sgx/stock/h78-hongkongland-usd/news-article',
            'C07': 'https://sginvestors.io/sgx/stock/c07-jardine-cnc/news-article',
            'J36': 'https://sginvestors.io/sgx/stock/j36-jmh-usd/news-article',
            'BN4': 'https://sginvestors.io/sgx/stock/bn4-keppel-corp/news-article',
            'AJBU': 'https://sginvestors.io/sgx/reit/ajbu-keppel-dc-reit/news-article',
            'N2IU': 'https://sginvestors.io/sgx/reit/n2iu-mapletree-com-tr/news-article',
            'ME8U': 'https://sginvestors.io/sgx/reit/me8u-mapletree-ind-tr/news-article',
            'M44U': 'https://sginvestors.io/sgx/reit/m44u-mapletree-log-tr/news-article',
            'O39': 'https://sginvestors.io/sgx/stock/o39-ocbc-bank/news-article',
            'S58': 'https://sginvestors.io/sgx/stock/s58-sats/news-article',
            'S68': 'https://sginvestors.io/sgx/stock/s68-sgx/news-article',
            'C6L': 'https://sginvestors.io/sgx/stock/c6l-sia/news-article',
            'Z74': 'https://sginvestors.io/sgx/stock/z74-singtel/news-article',
            'S63': 'https://sginvestors.io/sgx/stock/s63-st-engineering/news-article',
            'Y92': 'https://sginvestors.io/sgx/stock/y92-thaibev/news-article',
            'U11': 'https://sginvestors.io/sgx/stock/u11-uob/news-article',
            'U14': 'https://sginvestors.io/sgx/stock/u14-uol/news-article',
            'V03': 'https://sginvestors.io/sgx/stock/v03-venture/news-article',
            'F34': 'https://sginvestors.io/sgx/stock/f34-wilmar-intl/news-article',
            'BS6': 'https://sginvestors.io/sgx/stock/bs6-yzj-shipbldg-sgd/news-article'
        }
        
        stocks = ['U96', 'A17U', 'C38U', 'C09', 'C52', 'D01', 'D05',
                'BUOU', 'G13', 'H78', 'C07', 'J36', 'BN4', 'AJBU',
                'N2IU', 'ME8U', 'M44U', 'O39', 'S58', 'S68', 'C6L', 
                'Z74', 'S63', 'Y92', 'U11', 'U14', 'V03', 'F34', 'BS6']
        
        for ticker in stocks:

            res = requests.get(mapper.get(ticker), headers=HEADERS)
            
            try:

                soup = BeautifulSoup(res.text, features="html.parser")

                news = soup.find_all('article', attrs={'class': 'stocknewsitem'})

                for i in news:

                    d = i.find('div', attrs={'class': 'publisheddate'}).text
                    d = datetime.strptime(d,  '%Y-%m-%d %H:%M:%S').date()

                    if d == ds:

                        tickers.append(ticker)

                        date.append(d)

                        t = i.find('div', attrs={'class': 'title'}).text
                        title.append(t)

                        dsc = i.find('div', attrs={'class': 'description'}).text
                        desc.append(dsc)

                        src = i.find('img').attrs['alt']
                        source.append(src)

                        url.append(mapper.get(ticker))

                    else:
                        continue

            except:
                print('Either no news for this ticker or request returns nothing')
            
        sgx_news = pd.DataFrame({'date': date,
                                'ticker': tickers, 
                                'title': title,
                                'source': source,
                                'description': desc,
                                'url': url})
        
        sgx_news['title'] = sgx_news['title'].replace({r'(@[A-Za-z0–9_]+)|[^\x00-\x7F]|[^\w\s]||http\S+':''}, regex=True)
        sgx_news['description'] = sgx_news['description'].replace({r'(@[A-Za-z0–9_]+)|[^\x00-\x7F]|[^\w\s]||http\S+':''}, regex=True)
        sgx_news = sgx_news.drop_duplicates(subset=['title', 'ticker'], keep='first')
        sgx_news.sort_values(by='date', axis=0, ascending=True, inplace=True)
        sgx_news.reset_index(drop=True, inplace=True)
        
        return sgx_news
    
    df = scrape()
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df.rename(columns={'date': 'p_date'})
    
    col_types = {'p_date': 'datetime64', 'ticker': 'string', 'title': 'string', 'source': 'string', 'description': 'string', 'url':'string'}
    df = df.astype(col_types)

    # send to BigQuery
    schema = [
        SchemaField('p_date', 'DATE', 'REQUIRED'),
        SchemaField('ticker', 'STRING', 'REQUIRED'),
        SchemaField('title', 'STRING', 'REQUIRED'),
        SchemaField('source', 'STRING', 'REQUIRED'),
        SchemaField('description', 'STRING', 'REQUIRED'),
        SchemaField('url', 'STRING', 'REQUIRED')
    ]

    client = Client()
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    client.load_table_from_dataframe(df, destination=f"lti_ods.ods_ticker_level_financial_news_scraping_results${ds.strftime('%Y%m%d')}", job_config=job_config)

    return f"Successful run for date: {ds}"