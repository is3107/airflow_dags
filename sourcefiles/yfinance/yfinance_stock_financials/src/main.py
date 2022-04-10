from datetime import date, datetime  
import pandas as pd
import html5lib
import json
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from google.cloud.bigquery import Client, LoadJobConfig, WriteDisposition, SchemaField

###DO NOT REMOVE IMPORTS BELOW###
import pyarrow

############### test small sample first ###############
tickers = ['D05.SI', 'C07.SI', 'J36.SI']

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

    ds = datetime.strptime(ds, "%Y-%m-%d").date()
    
    # get list of tickers
    # def get_sgx_tickers(tgt_website):

    #     req = requests.get(tgt_website, headers={'User-Agent': 'Mozilla/5.0'})

    #     soup = BeautifulSoup(req.content, 'html.parser')
    #     companies = soup.find_all('div', class_='stockname')
    #     dirty_list = [company.text for company in companies]

    #     sgx_tickers = []

    #     for i in dirty_list:
    #         x = i.split(" (SGX: ", 1)
    #         y = x[1]
    #         ticker = y.split(")", 1)
    #         sgx_tickers.append(ticker[0] + ".SI")

    #     return sgx_tickers

    # tickers = get_sgx_tickers('https://sginvestors.io/sgx/stock-listing/alpha')

    #######################################

    def get_key_stats(tgt_website):
        req = Request(tgt_website, headers={'User-Agent': 'Mozilla/5.0'})    
        stocks_table_data_list = pd.read_html(urlopen(req), flavor='html5lib')
        
        result_stocks_table_data = stocks_table_data_list[0]
        for stocks_table_data in stocks_table_data_list[1:]:
             result_stocks_table_data = result_stocks_table_data.append(stocks_table_data)
        return result_stocks_table_data.set_index(0).T

    stats = pd.DataFrame()
    statistics = []

    # list of tickers not on yfinance
    no_data_tickers_list = []

    for i in range(0, len(tickers)):
        try:
            values =(get_key_stats('https://sg.finance.yahoo.com/quote/'+ str(tickers[i]) +'/key-statistics?p='+ str(tickers[i])))
            statistics.append(values)
        except:
            no_data_tickers_list.append(tickers[i])
    
    # remove tickers not on yfinance
    for i in range(0, len(no_data_tickers_list)):
        tickers.remove(no_data_tickers_list[i])

    stats = stats.append(statistics)
    stats.reset_index(drop=True, inplace= True)
    stats.insert(loc=0, column='Ticker', value=pd.Series(tickers)) 
    stats.set_index(['Ticker'], inplace = True)

    # create Ticker dataframe
    ticker_f = pd.DataFrame(tickers, columns =['Ticker'])

    # merge Ticker to statistics
    stocks_table_data = pd.merge(ticker_f, stats, how='outer', on = 'Ticker', validate = "m:m")

    # Add Partition date column
    stocks_table_data['p_date']=ds

    # clean up col names 
    stocks_table_data.columns=stocks_table_data.columns.str.replace('/', '')
    stocks_table_data.columns=stocks_table_data.columns.str.replace(' ', '_')
    stocks_table_data.columns=stocks_table_data.columns.str.replace('(', '_')
    stocks_table_data.columns=stocks_table_data.columns.str.replace(')', '_')
    stocks_table_data.columns=stocks_table_data.columns.str.replace('-', '_')
    stocks_table_data.columns=stocks_table_data.columns.str.replace('%', 'percent')
    stocks_table_data.columns=stocks_table_data.columns.str.replace('&', 'n')

    # clean up col starting with digits
    stocks_table_data.columns=stocks_table_data.columns.str.replace('52', '_52')
    stocks_table_data.columns=stocks_table_data.columns.str.replace('50', '_50')
    stocks_table_data.columns=stocks_table_data.columns.str.replace('200', '_200')
    stocks_table_data.columns=stocks_table_data.columns.str.replace('5_year', '_5_year')

    # Setting the types for the columns
    col_types = {
        'p_date': 'datetime64',
        'Ticker': 'string',
        'Market_cap__intra_day_': 'string',
        'Enterprise_value': 'string',
        'Trailing_PE': 'float',
        'Forward_PE': 'float', 
        'PEG_Ratio__5_yr_expected_': 'float',
        'Pricesales__ttm_': 'float',
        'Pricebook__mrq_': 'float',
        'Enterprise_valuerevenue': 'float', 
        'Enterprise_valueEBITDA': 'float',
        'Beta__5Y_monthly_': 'float',
        '_52_week_change_3': 'string',
        'SnP_500__52_week_change_3': 'string',
        '_52_week_high_3': 'float', 
        '_52_week_low_3': 'float',
        '_50_day_moving_average_3': 'float',
        '_200_day_moving_average_3': 'float',
        'Avg_vol__3_month__3': 'string',
        'Avg_vol__10_day__3': 'string', 
        'Shares_outstanding_5': 'string',
        'Implied_shares_outstanding_6': 'string',
        'Float_8': 'string',
        'percent_held_by_insiders_1': 'string',
        'percent_held_by_institutions_1': 'string', 
        'Shares_short_4': 'string',
        'Short_ratio_4': 'float',
        'Short_percent_of_float_4': 'string',
        'Short_percent_of_shares_outstanding_4': 'string',
        'Shares_short__prior_month___4': 'string',
        'Forward_annual_dividend_rate_4': 'float',
        'Forward_annual_dividend_yield_4': 'string',
        'Trailing_annual_dividend_rate_3': 'float',
        'Trailing_annual_dividend_yield_3': 'string',
        '_5_year_average_dividend_yield_4': 'float',
        'Payout_ratio_4': 'string',
        'Dividend_date_3': 'string',
        'Ex_dividend_date_4': 'string',
        'Last_split_factor_2': 'string',
        'Last_split_date_3': 'string',
        'Fiscal_year_ends': 'string',
        'Most_recent_quarter__mrq_': 'string',
        'Profit_margin': 'string',
        'Operating_margin__ttm_': 'string',
        'Return_on_assets__ttm_': 'string',
        'Return_on_equity__ttm_': 'string',
        'Revenue__ttm_': 'string',
        'Revenue_per_share__ttm_': 'float',
        'Quarterly_revenue_growth__yoy_': 'string',
        'Gross_profit__ttm_': 'string',
        'EBITDA': 'string',
        'Net_income_avi_to_common__ttm_': 'string',
        'Diluted_EPS__ttm_': 'float',
        'Quarterly_earnings_growth__yoy_': 'string',
        'Total_cash__mrq_': 'string',
        'Total_cash_per_share__mrq_': 'string',
        'Total_debt__mrq_': 'string',
        'Total_debtequity__mrq_': 'float',
        'Current_ratio__mrq_': 'float',
        'Book_value_per_share__mrq_': 'float',
        'Operating_cash_flow__ttm_': 'string',
        'Levered_free_cash_flow__ttm_': 'string'
    }
    
    stocks_table_data = stocks_table_data.astype(col_types)

    schema = [
        SchemaField('p_date', 'DATE', 'REQUIRED'), 
        SchemaField('Ticker', 'STRING', 'REQUIRED'),
        SchemaField('Market_cap__intra_day_', 'STRING'),
        SchemaField('Enterprise_value', 'STRING'),
        SchemaField('Trailing_PE', 'FLOAT'),
        SchemaField('Forward_PE', 'FLOAT'),
        SchemaField('PEG_Ratio__5_yr_expected_', 'FLOAT'),
        SchemaField('Pricesales__ttm_', 'FLOAT'),
        SchemaField('Pricebook__mrq_', 'FLOAT'),
        SchemaField('Enterprise_valuerevenue', 'FLOAT'),
        SchemaField('Enterprise_valueEBITDA', 'FLOAT'),
        SchemaField('Beta__5Y_monthly_', 'FLOAT'),
        SchemaField('_52_week_change_3', 'STRING'),
        SchemaField('SnP_500__52_week_change_3', 'STRING'),
        SchemaField('_52_week_high_3', 'FLOAT'),
        SchemaField('_52_week_low_3', 'FLOAT'),
        SchemaField('_50_day_moving_average_3', 'FLOAT'),
        SchemaField('_200_day_moving_average_3', 'FLOAT'),
        SchemaField('Avg_vol__3_month__3', 'STRING'),
        SchemaField('Avg_vol__10_day__3', 'STRING'),
        SchemaField('Shares_outstanding_5', 'STRING'),
        SchemaField('Implied_shares_outstanding_6', 'STRING'),
        SchemaField('Float_8', 'STRING'),
        SchemaField('percent_held_by_insiders_1', 'STRING'),
        SchemaField('percent_held_by_institutions_1', 'STRING'),
        SchemaField('Shares_short_4', 'STRING'),
        SchemaField('Short_ratio_4', 'FLOAT'),
        SchemaField('Short_percent_of_float_4', 'STRING'),
        SchemaField('Short_percent_of_shares_outstanding_4', 'STRING'),
        SchemaField('Shares_short__prior_month___4', 'STRING'),
        SchemaField('Forward_annual_dividend_rate_4', 'FLOAT'),
        SchemaField('Forward_annual_dividend_yield_4', 'STRING'),
        SchemaField('Trailing_annual_dividend_rate_3', 'FLOAT'),
        SchemaField('Trailing_annual_dividend_yield_3', 'STRING'),
        SchemaField('_5_year_average_dividend_yield_4', 'FLOAT'),
        SchemaField('Payout_ratio_4', 'STRING'),
        SchemaField('Dividend_date_3', 'STRING'),
        SchemaField('Ex_dividend_date_4', 'STRING'),
        SchemaField('Last_split_factor_2', 'STRING'),
        SchemaField('Last_split_date_3', 'STRING'),
        SchemaField('Fiscal_year_ends', 'STRING'),
        SchemaField('Most_recent_quarter__mrq_', 'STRING'),
        SchemaField('Profit_margin', 'STRING'),
        SchemaField('Operating_margin__ttm_', 'STRING'),
        SchemaField('Return_on_assets__ttm_', 'STRING'),
        SchemaField('Return_on_equity__ttm_', 'STRING'),
        SchemaField('Revenue__ttm_', 'STRING'),
        SchemaField('Revenue_per_share__ttm_', 'FLOAT'),
        SchemaField('Quarterly_revenue_growth__yoy_', 'STRING'),
        SchemaField('Gross_profit__ttm_', 'STRING'),
        SchemaField('EBITDA', 'STRING'),
        SchemaField('Net_income_avi_to_common__ttm_', 'STRING'),
        SchemaField('Diluted_EPS__ttm_', 'FLOAT'),
        SchemaField('Quarterly_earnings_growth__yoy_', 'STRING'),
        SchemaField('Total_cash__mrq_', 'STRING'),
        SchemaField('Total_cash_per_share__mrq_', 'STRING'),
        SchemaField('Total_debt__mrq_', 'STRING'),
        SchemaField('Total_debtequity__mrq_', 'FLOAT'),
        SchemaField('Current_ratio__mrq_', 'FLOAT'),
        SchemaField('Book_value_per_share__mrq_', 'FLOAT'),
        SchemaField('Operating_cash_flow__ttm_', 'STRING'),
        SchemaField('Levered_free_cash_flow__ttm_', 'STRING')
    ]

    client = Client()
    job_config = LoadJobConfig(write_disposition=WriteDisposition.WRITE_TRUNCATE, schema=schema)
    client.load_table_from_dataframe(stocks_table_data, destination=f"lti_ods.ods_yfinance_stock_financials_daily${ds.strftime('%Y%m%d')}", job_config=job_config)
    
    return f"Successful run for date: {ds}"