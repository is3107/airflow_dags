from datetime import datetime, timedelta  
import yfinance as yf
import json
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField, QueryJobConfig, ScalarQueryParameter

###DO NOT REMOVE IMPORTS BELOW###
import pyarrow

# python functions
def pull_yfinance_data(request):
    
    request_json = request.get_json(silent=True)
    request_args = request.args
    request_data = request.data

    if request_json and 'ds' in request_json:
        ds = request_json['ds']
    elif request_args and 'ds' in request_args:
        ds = request_args['ds']
    elif request_data:
        ds = json.loads(request_data)['ds']

    ds_string = ds
    ds = datetime.strptime(ds, "%Y-%m-%d").date()
    tmr_ds_string = datetime.strftime(ds + timedelta(days=1), "%Y-%m-%d")


    # get list of tickers
    query_string = """
    SELECT CONCAT(Stock_Code, ".SI") AS stock_code FROM `is3107.lti_ods.ods_sgx_stock_price_daily`
    WHERE p_date = @ds
    """
    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("ds", "DATE", ds),   
        ]
    )

    tickers = (
        Client().query(query_string, job_config=job_config)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    stock_codes = tickers["stock_code"].tolist()

    if len(stock_codes) == 0:
        # Weekend/Holiday -> hardcode to get empty df column names
        stock_codes_str = "D05.SI U11.SI"
    else:
        stock_codes_str = " ".join(stock_codes)
    
    # Get Price data from yfinance
    prices = yf.download(stock_codes_str, start=ds_string, end=tmr_ds_string, group_by='Ticker')
    stocks_table_data = prices.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1).reset_index().drop("Date",axis=1)

    # Clear hardcoded data from dataframe for Weekends/Holidays
    if len(stock_codes) == 0:
        stocks_table_data = stocks_table_data.iloc[0:0]

    # Add Partition date column
    stocks_table_data['p_date']=ds

    # clean up col names 
    stocks_table_data.columns=stocks_table_data.columns.str.replace(' ', '_')

    # clean up Tickers
    stocks_table_data["Ticker"] = stocks_table_data["Ticker"].apply(lambda x: x.split(".")[0])

    # Setting the types for the columns
    col_types = {
        'p_date': 'datetime64',
        'Ticker': 'string',
        'Open': 'float',
        'High': 'float',
        'Low': 'float',
        'Close': 'float', 
        'Adj_Close': 'float',
        'Volume': 'int' 
    }
    
    stocks_table_data = stocks_table_data.astype(col_types)

    schema = [
        SchemaField('p_date', 'DATE', 'REQUIRED'), 
        SchemaField('Ticker', 'STRING', 'REQUIRED'),
        SchemaField('Open', 'FLOAT'),
        SchemaField('High', 'FLOAT'),
        SchemaField('Low', 'FLOAT'),
        SchemaField('Close', 'FLOAT'),
        SchemaField('Adj_Close', 'FLOAT'),
        SchemaField('Volume', 'INTEGER')
    ]

    client = Client()
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    client.load_table_from_dataframe(stocks_table_data, destination=f"lti_ods.ods_yfinance_stock_prices_daily${ds.strftime('%Y%m%d')}", job_config=job_config)
    
    return f"Successful run for date: {ds}"