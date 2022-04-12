import requests
import pandas as pd

from datetime import datetime

from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField
import pyarrow

def get_stock_list(request):
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

    
    req = requests.get('https://api.sgx.com/securities/v1.1/gtis?start=0&size=250')
    pages = req.json()['meta']['totalPages']

    stocks = []
    for i in range(pages):
        req = requests.get('https://api.sgx.com/securities/v1.1/gtis?start={}&size=250'.format(i))
        page = req.json()['data']
        page_date = datetime.fromtimestamp(req.json()['meta']['processedTime']/1000)

        for stock in page:
            stock['p_date'] = ds

        stocks.extend(page)
    
    stockList = pd.DataFrame(stocks)
    stockList.columns = ('issue', 'year', 'company_name', 'rank', 'adjustment', 'base_score', 'total_score', 'stock_code', 'isin_code', 'p_date')
    
    col_types = {
        'issue': 'string', 
        'year': 'int', 
        'company_name': 'string',
        'rank': 'int',
        'adjustment': 'int',
        'base_score': 'float',
        'total_score': 'float',
        'stock_code': 'str',
        'isin_code': 'str',
        'p_date':'datetime64'
        }
    stockList = stockList.astype(col_types)

    # send to BigQuery
    schema = [
        SchemaField('issue',        'STRING',   'NULLABLE'),
        SchemaField('year',         'INTEGER',  'NULLABLE'),
        SchemaField('company_name', 'STRING',   'NULLABLE'),
        SchemaField('rank',         'INTEGER',  'NULLABLE'),
        SchemaField('adjustment',   'INTEGER',  'NULLABLE'),
        SchemaField('base_score',   'FLOAT',    'NULLABLE'),
        SchemaField('total_score',  'FLOAT',    'NULLABLE'),
        SchemaField('stock_code',   'STRING',   'REQUIRED'),
        SchemaField('isin_code',    'STRING',   'NULLABLE'),
        SchemaField('p_date',       'DATE',     'REQUIRED')

    ]

    client = Client()
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    client.load_table_from_dataframe(stockList, destination=f"lti_dim.dim_sgx_stock_list_daily${ds.strftime('%Y%m%d')}", job_config=job_config)

    return f"Successful run for date: {ds}"