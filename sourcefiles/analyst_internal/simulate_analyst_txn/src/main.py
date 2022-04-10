import pandas as pd
from finvizfinance.quote import Quote
from random import randint, uniform, randrange, seed, sample
from datetime import datetime, timedelta, time
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField, QueryJobConfig, ScalarQueryParameter
from holidays import country_holidays
import numpy as np


###DO NOT REMOVE IMPORTS BELOW###
import pyarrow

def simulate_txn(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    request_data = request.data

    if request_json and 'ds' in request_json:
        ds = request_json['ds']
    elif request_args and 'ds' in request_args:
        ds = request_args['ds']
    elif request_data:
        ds = json.loads(request_data)['ds']

    ds = datetime.strptime(ds, '%Y-%m-%d').date()

    # Set seed for reproducibility on date
    seed(int(ds.strftime('%Y%m%d')))

    # get tickers from SGX stock code table
    query_string = """
        SELECT * FROM `is3107.lti_dwd.dwd_stock_prices_daily`
        WHERE p_date = @ds
        AND Volume > 0
        """
    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("ds", "DATE", ds),   
        ]
    )

    df = (
        Client().query(query_string, job_config=job_config)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    sg_tickers = df["Stock_Code"].tolist()

    # Only process and load data into BigQuery if its a trading day
    sg_holidays = country_holidays('SG', years=ds.year)
    if np.is_busday(ds, holidays=list(sg_holidays.keys())):

        txn = []
        # simulate buys and sells 
        num_txn = randint(50, 500)
        # Use sample to ensure timestamps are unique
        timedeltas = sample(range(0, 1440), num_txn)
        for i in range(0, num_txn):
            # pick an analyst
            a = randint(1, 13)
            # pick a stock 
            s = sg_tickers [randint(0, len(sg_tickers)-1)]
            # get the true price
            high = float(df[df['Stock_Code'] == s].iloc[0]['High'])
            low = float(df[df['Stock_Code'] == s].iloc[0]['Low'])
            # Get a random price between high and low 
            txn_price  = round(uniform(low, high),3)
            # 50/50 chance to buy or sell
            txn_type = 'BUY' if uniform(0,1) > 0.5 else 'SELL'
            amt = randint(1, 100)
            ts = datetime.combine(ds, time()) + timedelta(minutes=timedeltas[i])
            txn.append({'Analyst': a, 'Ticker': s, 'Price': txn_price, 'Txn_Type': txn_type, 'Amount':amt, 'Txn_Time': ts})

        txn_df = pd.DataFrame(txn)

        # adding p_date columns
        txn_df['p_date'] = ds

        # defining col types
        col_types = {'Analyst':'int32', 'Ticker':'string', 'Txn_Type':'string', 'Price':'float', 'Amount': 'float', 
                    'Txn_Time': 'datetime64', 'p_date' : 'datetime64'}
        txn_df = txn_df.astype(col_types)


        schema = [SchemaField('p_date', 'DATE', 'REQUIRED'), 
            SchemaField('Analyst', 'INTEGER', 'REQUIRED'),
            SchemaField('Txn_Type', 'STRING', 'REQUIRED'),
            SchemaField('Ticker', 'STRING', 'REQUIRED'),
            SchemaField('Price', 'FLOAT', 'REQUIRED'),
            SchemaField('Amount', 'FLOAT', 'REQUIRED'),
            SchemaField('Txn_Time', 'DATETIME', 'REQUIRED'),
        ]

        client = Client()
        job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
        client.load_table_from_dataframe(txn_df, destination=f"lti_ods.ods_internal_analyst_txn_daily${ds.strftime('%Y%m%d')}", job_config=job_config)
    
    return f"Successful run for date: {ds}"