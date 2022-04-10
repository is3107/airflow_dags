import requests
import pandas as pd

from datetime import datetime

from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField, TableReference, ScalarQueryParameter, QueryJobConfig
import pyarrow

def get_stock_prices(request):
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

    client = Client()

    # query for ODS table
    query_string = """
    SELECT DISTINCT stock_code FROM `is3107.lti_ods.ods_sgx_stock_price_daily`
    WHERE p_date = (
        SELECT MAX(p_date)
        FROM `is3107.lti_ods.ods_sgx_stock_price_daily`
    )
    """

    # set job config
    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("ds", "DATE", ds),   
        ]
    )

    # push result from ods as dataframe
    df = (
        client.query(query_string, job_config=job_config)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )

    stock_prices = []

    for sc in df['stock_code']:
        req = requests.get(f'https://api.sgx.com/securities/v1.1/charts/intraday/stocks/code/{sc}/1d')
        stock_prices.extend(req.json()['data']['intraday'])

    stock_prices = pd.DataFrame(stock_prices)
    stock_prices['p_date'] = ds 

    col_types = {
        'cur': 'string', 
        'pv': 'float', 
        'bond_dirty_price': 'string',
        'lt': 'float',
        'trading_time': 'string',
        'type': 'string',
        'dp': 'string',
        'du': 'string',
        'dpc': 'string',
        'ig':'string',
        'bond_clean_price': 'string',
        'ed': 'string',
        'change_vs_pc_percentage': 'string',
        'ptd': 'string',
        'h': 'float',
        'l': 'float',
        'n': 'string',
        'o': 'float',
        'change_vs_pc': 'string',
        'bond_accrued_interest': 'string',
        'nc': 'string',
        'v': 'float',
        'vl': 'float',
        'lf': 'string',
        'bond_date': 'string',
        'p_date': 'datetime64' 
        }

    stock_prices = stock_prices.astype(col_types)

    # send to BigQuery
    schema = [
        SchemaField('cur',                  'STRING',   'NULLABLE'),
        SchemaField('pv',                   'FLOAT',    'NULLABLE'),
        SchemaField('bond_dirty_price',     'STRING',   'NULLABLE'),
        SchemaField('lt',                   'FLOAT',    'NULLABLE'),
        SchemaField('trading_time',         'STRING',   'NULLABLE'),
        SchemaField('type',                 'STRING',   'NULLABLE'),
        SchemaField('dp',                   'STRING',   'NULLABLE'),
        SchemaField('du',                   'STRING',   'NULLABLE'),
        SchemaField('dpc',                  'STRING',   'NULLABLE'),
        SchemaField('ig',                   'STRING',   'NULLABLE'),
        SchemaField('bond_clean_price',     'STRING',   'NULLABLE'),
        SchemaField('ed',                   'STRING',   'NULLABLE'),
        SchemaField('change_vs_pc_percentage','STRING', 'NULLABLE'),
        SchemaField('ptd',                  'STRING',   'NULLABLE'),
        SchemaField('h',                    'FLOAT',    'NULLABLE'),
        SchemaField('l',                    'FLOAT',    'NULLABLE'),
        SchemaField('n',                    'STRING',   'NULLABLE'),
        SchemaField('o',                    'FLOAT',    'NULLABLE'),
        SchemaField('change_vs_pc',         'STRING',   'NULLABLE'),
        SchemaField('bond_accrued_interest','STRING',   'NULLABLE'),
        SchemaField('nc',                   'STRING',   'NULLABLE'),
        SchemaField('v',                    'FLOAT',    'NULLABLE'),
        SchemaField('vl',                   'FLOAT',    'NULLABLE'),
        SchemaField('lf',                   'STRING',   'NULLABLE'),
        SchemaField('bond_date',            'STRING',   'NULLABLE'),
        SchemaField('p_date',               'DATE',     'NULLABLE')

    ]

    
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    client.load_table_from_dataframe(stockList, destination=f"lti_ods.ods_sgx_stock_chart_1d${ds.strftime('%Y%m%d')}", job_config=job_config)

    return f"Successful run for date: {ds}"