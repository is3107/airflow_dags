from datetime import date, datetime  
import pandas as pd
import html5lib
import requests
import json
import time
import logging
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField, TableReference, ScalarQueryParameter, QueryJobConfig

###DO NOT REMOVE IMPORTS BELOW###
import pyarrow

bqclient = Client()

def dwd_yfinance_data(request):

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

    # query for ODS table
    query_string = """
    SELECT * FROM `is3107.lti_ods.ods_yfinance_stock_financials_daily`
    WHERE p_date = @ds
    """

    # set job config
    job_config = QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("ds", "DATE", ds),   
        ]
    )

    # push result from ods as dataframe
    df = (
        bqclient.query(query_string, job_config=job_config)
        .result()
        .to_dataframe(
            create_bqstorage_client=True,
        )
    )

    # partition date
    df['p_date'] = ds
    
    ############### fix data types and values ###############
    # percentage based strings -> convert to float
    df['_52_week_change_3'] = df['_52_week_change_3'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['SnP_500__52_week_change_3'] = df['SnP_500__52_week_change_3'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['percent_held_by_insiders_1'] = df['percent_held_by_insiders_1'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['percent_held_by_institutions_1'] = df['percent_held_by_institutions_1'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Short_percent_of_float_4'] = df['Short_percent_of_float_4'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Short_percent_of_shares_outstanding_4'] = df['Short_percent_of_shares_outstanding_4'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Forward_annual_dividend_yield_4'] = df['Forward_annual_dividend_yield_4'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Trailing_annual_dividend_yield_3'] = df['Trailing_annual_dividend_yield_3'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Payout_ratio_4'] = df['Payout_ratio_4'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Profit_margin'] = df['Profit_margin'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Operating_margin__ttm_'] = df['Operating_margin__ttm_'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Return_on_assets__ttm_'] = df['Return_on_assets__ttm_'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Return_on_equity__ttm_'] = df['Return_on_equity__ttm_'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Quarterly_revenue_growth__yoy_'] = df['Quarterly_revenue_growth__yoy_'].str.replace(',','').str.rstrip('%').astype('float') / 100.0
    df['Quarterly_earnings_growth__yoy_'] = df['Quarterly_earnings_growth__yoy_'].str.replace(',','').str.rstrip('%').astype('float') / 100.0

    # strings with M & B suffix for million/billion -> convert to float
    df['Market_cap__intra_day_'] = df['Market_cap__intra_day_'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Enterprise_value'] = df['Enterprise_value'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Avg_vol__3_month__3'] = df['Avg_vol__3_month__3'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Avg_vol__10_day__3'] = df['Avg_vol__10_day__3'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Shares_outstanding_5'] = df['Shares_outstanding_5'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Implied_shares_outstanding_6'] = df['Implied_shares_outstanding_6'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Float_8'] = df['Float_8'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Shares_short_4'] = df['Shares_short_4'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Shares_short__prior_month___4'] = df['Shares_short__prior_month___4'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Revenue__ttm_'] = df['Revenue__ttm_'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Gross_profit__ttm_'] = df['Gross_profit__ttm_'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['EBITDA'] = df['EBITDA'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Net_income_avi_to_common__ttm_'] = df['Net_income_avi_to_common__ttm_'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Total_cash__mrq_'] = df['Total_cash__mrq_'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Total_debt__mrq_'] = df['Total_debt__mrq_'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Operating_cash_flow__ttm_'] = df['Operating_cash_flow__ttm_'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)
    df['Levered_free_cash_flow__ttm_'] = df['Levered_free_cash_flow__ttm_'].fillna('0').replace({'k': '*1e3', 'M': '*1e6', 'B': '*1e9'}, regex=True).map(pd.eval).astype(float)

    # ratio strings -> convert to float
    def ratio_to_float (x):
        if type(x) == str:
            x = x.split(':')
            if len(x) == 2:
                a, b = x
                c = int(a)/int(b)
                return c

        return None 

    df['Short_ratio_4'] = df['Short_ratio_4'].apply(ratio_to_float)
    df['Last_split_factor_2'] = df['Last_split_factor_2'].apply(ratio_to_float)

    # removed columns for fiscal/dividend payout dates as these are not categorical in nature
    # 5 columns removed: Dividend_date_3, Ex_dividend_date_4, Last_split_date_3, Fiscal_year_ends, Most_recent_quarter__mrq_
    df.drop(['Dividend_date_3', 'Ex_dividend_date_4', 'Last_split_date_3', 'Fiscal_year_ends', 'Most_recent_quarter__mrq_'], axis=1, inplace=True)

    # define column types
    col_types = {
        'p_date': 'datetime64',
        'Ticker': 'string',
        'Market_cap__intra_day_': 'float',
        'Enterprise_value': 'float',
        'Trailing_PE': 'float',
        'Forward_PE': 'float', 
        'PEG_Ratio__5_yr_expected_': 'float',
        'Pricesales__ttm_': 'float',
        'Pricebook__mrq_': 'float',
        'Enterprise_valuerevenue': 'float', 
        'Enterprise_valueEBITDA': 'float',
        'Beta__5Y_monthly_': 'float',
        '_52_week_change_3': 'float',
        'SnP_500__52_week_change_3': 'float',
        '_52_week_high_3': 'float', 
        '_52_week_low_3': 'float',
        '_50_day_moving_average_3': 'float',
        '_200_day_moving_average_3': 'float',
        'Avg_vol__3_month__3': 'float',
        'Avg_vol__10_day__3': 'float', 
        'Shares_outstanding_5': 'float',
        'Implied_shares_outstanding_6': 'float',
        'Float_8': 'float',
        'percent_held_by_insiders_1': 'float',
        'percent_held_by_institutions_1': 'float', 
        'Shares_short_4': 'float',
        'Short_ratio_4': 'float',
        'Short_percent_of_float_4': 'float',
        'Short_percent_of_shares_outstanding_4': 'float',
        'Shares_short__prior_month___4': 'float',
        'Forward_annual_dividend_rate_4': 'float',
        'Forward_annual_dividend_yield_4': 'float',
        'Trailing_annual_dividend_rate_3': 'float',
        'Trailing_annual_dividend_yield_3': 'float',
        '_5_year_average_dividend_yield_4': 'float',
        'Payout_ratio_4': 'float',
        'Last_split_factor_2': 'float',
        'Profit_margin': 'float',
        'Operating_margin__ttm_': 'float',
        'Return_on_assets__ttm_': 'float',
        'Return_on_equity__ttm_': 'float',
        'Revenue__ttm_': 'float',
        'Revenue_per_share__ttm_': 'float',
        'Quarterly_revenue_growth__yoy_': 'float',
        'Gross_profit__ttm_': 'float',
        'EBITDA': 'float',
        'Net_income_avi_to_common__ttm_': 'float',
        'Diluted_EPS__ttm_': 'float',
        'Quarterly_earnings_growth__yoy_': 'float',
        'Total_cash__mrq_': 'float',
        'Total_cash_per_share__mrq_': 'float',
        'Total_debt__mrq_': 'float',
        'Total_debtequity__mrq_': 'float',
        'Current_ratio__mrq_': 'float',
        'Book_value_per_share__mrq_': 'float',
        'Operating_cash_flow__ttm_': 'float',
        'Levered_free_cash_flow__ttm_': 'float'
    }

    df = df.astype(col_types)
    
    schema = [
        SchemaField('p_date', 'DATE', 'REQUIRED'), 
        SchemaField('Ticker', 'STRING', 'REQUIRED'),
        SchemaField('Market_cap__intra_day_', 'FLOAT'),
        SchemaField('Enterprise_value', 'FLOAT'),
        SchemaField('Trailing_PE', 'FLOAT'),
        SchemaField('Forward_PE', 'FLOAT'),
        SchemaField('PEG_Ratio__5_yr_expected_', 'FLOAT'),
        SchemaField('Pricesales__ttm_', 'FLOAT'),
        SchemaField('Pricebook__mrq_', 'FLOAT'),
        SchemaField('Enterprise_valuerevenue', 'FLOAT'),
        SchemaField('Enterprise_valueEBITDA', 'FLOAT'),
        SchemaField('Beta__5Y_monthly_', 'FLOAT'),
        SchemaField('_52_week_change_3', 'FLOAT'),
        SchemaField('SnP_500__52_week_change_3', 'FLOAT'),
        SchemaField('_52_week_high_3', 'FLOAT'),
        SchemaField('_52_week_low_3', 'FLOAT'),
        SchemaField('_50_day_moving_average_3', 'FLOAT'),
        SchemaField('_200_day_moving_average_3', 'FLOAT'),
        SchemaField('Avg_vol__3_month__3', 'FLOAT'),
        SchemaField('Avg_vol__10_day__3', 'FLOAT'),
        SchemaField('Shares_outstanding_5', 'FLOAT'),
        SchemaField('Implied_shares_outstanding_6', 'FLOAT'),
        SchemaField('Float_8', 'FLOAT'),
        SchemaField('percent_held_by_insiders_1', 'FLOAT'),
        SchemaField('percent_held_by_institutions_1', 'FLOAT'),
        SchemaField('Shares_short_4', 'FLOAT'),
        SchemaField('Short_ratio_4', 'FLOAT'),
        SchemaField('Short_percent_of_float_4', 'FLOAT'),
        SchemaField('Short_percent_of_shares_outstanding_4', 'FLOAT'),
        SchemaField('Shares_short__prior_month___4', 'FLOAT'),
        SchemaField('Forward_annual_dividend_rate_4', 'FLOAT'),
        SchemaField('Forward_annual_dividend_yield_4', 'FLOAT'),
        SchemaField('Trailing_annual_dividend_rate_3', 'FLOAT'),
        SchemaField('Trailing_annual_dividend_yield_3', 'FLOAT'),
        SchemaField('_5_year_average_dividend_yield_4', 'FLOAT'),
        SchemaField('Payout_ratio_4', 'FLOAT'),  
        SchemaField('Last_split_factor_2', 'FLOAT'),
        SchemaField('Profit_margin', 'FLOAT'),
        SchemaField('Operating_margin__ttm_', 'FLOAT'),
        SchemaField('Return_on_assets__ttm_', 'FLOAT'),
        SchemaField('Return_on_equity__ttm_', 'FLOAT'),
        SchemaField('Revenue__ttm_', 'FLOAT'),
        SchemaField('Revenue_per_share__ttm_', 'FLOAT'),
        SchemaField('Quarterly_revenue_growth__yoy_', 'FLOAT'),
        SchemaField('Gross_profit__ttm_', 'FLOAT'),
        SchemaField('EBITDA', 'FLOAT'),
        SchemaField('Net_income_avi_to_common__ttm_', 'FLOAT'),
        SchemaField('Diluted_EPS__ttm_', 'FLOAT'),
        SchemaField('Quarterly_earnings_growth__yoy_', 'FLOAT'),
        SchemaField('Total_cash__mrq_', 'FLOAT'),
        SchemaField('Total_cash_per_share__mrq_', 'FLOAT'),
        SchemaField('Total_debt__mrq_', 'FLOAT'),
        SchemaField('Total_debtequity__mrq_', 'FLOAT'),
        SchemaField('Current_ratio__mrq_', 'FLOAT'),
        SchemaField('Book_value_per_share__mrq_', 'FLOAT'),
        SchemaField('Operating_cash_flow__ttm_', 'FLOAT'),
        SchemaField('Levered_free_cash_flow__ttm_', 'FLOAT')
    ]

    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    load_job = bqclient.load_table_from_dataframe(df, destination=f"lti_dwd.dwd_yfinance_stock_financials_daily${ds.strftime('%Y%m%d')}", job_config=job_config)

    while load_job.running():
        time.sleep(0.1)

    error = load_job.error_result

    if error is not None:
        logging.error(error)

    return f"Successful for date {ds}"