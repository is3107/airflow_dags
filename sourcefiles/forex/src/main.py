#import libraries to handle request to api
import requests
import json
from datetime import datetime
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField
import pandas as pd
from datetime import datetime

from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField, QueryJobConfig, ScalarQueryParameter, enums
import pyarrow

def get_forex(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    request_data = request.data

    if request_json and 'ds' in request_json:
        ds = request_json['ds']
    elif request_args and 'ds' in request_args:
        ds = request_args['ds']
    elif request_data:
        ds = json.loads(request_data)['ds']

    # need ds as a string for api request
    ds_str = ds
    ds = datetime.strptime(ds, "%Y-%m-%d").date()

    # Get all Currency Codes used on SGX for today
    query_string = """
    SELECT DISTINCT Currency FROM `is3107.lti_ods.ods_sgx_stock_price_daily`
    WHERE p_date = @ds
    """
    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("ds", "DATE", ds),   
        ]
    )

    currency_code_df = (
        Client().query(query_string, job_config=job_config)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )
    currency_code = currency_code_df["Currency"].tolist()
    rate_list = []

    for base in currency_code:
        out_curr= "SGD"

        # set start and end date as itself
        start_date = ds_str
        end_date = ds_str

        url = f"https://api.exchangerate.host/timeseries?base={base}&start_date={start_date}&end_date={end_date}&symbols={out_curr}"
        response = requests.get(url)

        # get data in json format 
        data = response.json()

        # get USDSGD rate
        rate = data['rates'][ds_str][out_curr]
        rate_list.append(rate)

    # put into dataframe for upload to BQ
    df = pd.DataFrame({'Currency': currency_code, "Rate": rate_list, 'p_date' : ds})

    # insert into BQ
    col_types = {
        "Currency": "string",
        'p_date': 'datetime64',
        'Rate': 'float'
    }

    df = df.astype(col_types)

    schema = [
        SchemaField("Currency", enums.SqlTypeNames.STRING, "REQUIRED"),
        SchemaField("Rate", enums.SqlTypeNames.FLOAT, "REQUIRED"),
        SchemaField("p_date", enums.SqlTypeNames.DATE, "REQUIRED"),
    ]


    client = Client()
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    client.load_table_from_dataframe(df, destination=f"lti_dwd.dwd_exchange_rates_daily${ds.strftime('%Y%m%d')}", job_config=job_config)

    return f"Run Successful on {ds}"