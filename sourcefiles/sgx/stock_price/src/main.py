import requests
import pandas as pd
import io
import numpy as np
import traceback
import logging
from datetime import datetime, date, timedelta
from holidays import country_holidays

from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField, enums
import pyarrow

def get_stock_price(request):
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

    try:
        # Calculate SGX Historical Price File Number
        BASELINE_DATE = date(2022, 4, 1)
        BASELINE_COUNTER = 6188
        years = list(range(BASELINE_DATE.year, ds.year + 1)) if BASELINE_DATE <= ds else list(range(ds.year, BASELINE_DATE.year + 1))
        sg_holidays = country_holidays('SG', years=years)

        date_diff = np.busday_count(BASELINE_DATE, ds + timedelta(days=1), holidays=list(sg_holidays.keys()))
        file_number = BASELINE_COUNTER + date_diff - 1

        # Request Headers
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
        }

        # Get Price Data from SGX
        url = f"https://links.sgx.com/1.0.0/securities-historical/{file_number}/SESprice.dat"
        r = requests.get(url, headers=headers)


        # Feed into DataFrame
        data = io.StringIO(r.text)
        col_names = ["Trade_Date", "Stock_Name", "Remarks", "Currency", "High", "Low", "Last", "Change", "Volume", "Bid", "Offer", "Market", "Open", "Value", "Stock_Code", "DClose"]
        df = pd.read_csv(data, sep=";", names=col_names, index_col=False)
        df['p_date'] = ds

        col_types = {
            "Stock_Name": "string",
            "Remarks": "string",
            "Currency": "string",
            "Market": "string",
            "Stock_Code": "string",
            "Trade_Date": "datetime64",
            "p_date": "datetime64"
        }

        df[list(col_types.keys())] = df[list(col_types.keys())].astype(str).apply(lambda x: x.str.strip())
        df = df.astype(col_types)

        # Drop all data on weekends/holidays
        if not np.is_busday(ds, holidays=list(sg_holidays.keys())):
            df = df[0:0] 

        # Ingest into BigQuery
        schema = [
            SchemaField("Trade_Date", enums.SqlTypeNames.DATE, "REQUIRED"),
            SchemaField("Stock_Name", enums.SqlTypeNames.STRING, "REQUIRED"),
            SchemaField("Remarks", enums.SqlTypeNames.STRING, "NULLABLE"),
            SchemaField("Currency", enums.SqlTypeNames.STRING, "REQUIRED"),
            SchemaField("High", enums.SqlTypeNames.FLOAT, "REQUIRED"),
            SchemaField("Low", enums.SqlTypeNames.FLOAT, "REQUIRED"),
            SchemaField("Last", enums.SqlTypeNames.FLOAT, "REQUIRED"),
            SchemaField("Change", enums.SqlTypeNames.FLOAT, "REQUIRED"),
            SchemaField("Volume", enums.SqlTypeNames.INTEGER, "REQUIRED"),
            SchemaField("Bid", enums.SqlTypeNames.FLOAT, "REQUIRED"),
            SchemaField("Offer", enums.SqlTypeNames.FLOAT, "REQUIRED"),
            SchemaField("Market", enums.SqlTypeNames.STRING, "REQUIRED"),
            SchemaField("Open", enums.SqlTypeNames.FLOAT, "REQUIRED"),
            SchemaField("Value", enums.SqlTypeNames.INTEGER, "REQUIRED"),
            SchemaField("Stock_Code", enums.SqlTypeNames.STRING, "REQUIRED"),
            SchemaField("DClose", enums.SqlTypeNames.FLOAT, "REQUIRED"),
            SchemaField("p_date", enums.SqlTypeNames.DATE, "REQUIRED")
        ]
        job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
        Client().load_table_from_dataframe(df, destination=f"lti_ods.ods_sgx_stock_price_daily${ds.strftime('%Y%m%d')}", job_config=job_config)

        return f"Successful run for date: {ds}"

    except Exception as e:
        error_message = traceback.format_exc().replace('\n', '  ')
        logging.error(error_message)
        raise e
