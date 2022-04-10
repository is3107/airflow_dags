"""
Paths class is a common ground to update all paths across DAGs
"""
class Paths():
    FINVIZ_STOCK_PERF = '/home/is3107/airflow/dags/sourcefiles/finviz/finviz_stock_perf'
    FINVIZ_STOCK_OVW = '/home/is3107/airflow/dags/sourcefiles/finviz/finviz_stock_ovw'
    FINVIZ_STOCK_FIN = '/home/is3107/airflow/dags/sourcefiles/finviz/finviz_stock_fin'
    SIMULATE_INTERNAL_TXN = '/home/is3107/airflow/dags/sourcefiles/analyst_internal/simulate_analyst_txn'
    ANALYST_PORTFOLIO_PERF = '/home/is3107/airflow/dags/sourcefiles/analyst_internal/analyst_portfolio_perf'
    GENERATE_REQ_SCRIPT = '/home/is3107/airflow/dags/templates/scripts/generate_requirements.sh'
    YFINANCE_STOCK_FINANCIALS = '/home/is3107/airflow/dags/sourcefiles/yfinance/yfinance_stock_financials'
    YFINANCE_STOCK_FINANCIALS_DWD = '/home/is3107/airflow/dags/sourcefiles/yfinance/yfinance_stock_financials_dwd'
    YFINANCE_STOCK_PRICES = '/home/is3107/airflow/dags/sourcefiles/yfinance/yfinance_stock_prices'
    REDDIT_SCRAPING = '/home/is3107/airflow/dags/sourcefiles/reddit/scraping'
    REDDIT_PREPROCESSING = '/home/is3107/airflow/dags/sourcefiles/reddit/preprocessing'
    NEWS_SCRAPING = '/home/is3107/airflow/dags/sourcefiles/news/scraping'
    NEWS_PREPROCESSING = '/home/is3107/airflow/dags/sourcefiles/news/preprocessing'
    TWITTER_SCRAPING = '/home/is3107/airflow/dags/sourcefiles/twitter/scraping'
    TWITTER_PREPROCESSING = '/home/is3107/airflow/dags/sourcefiles/twitter/preprocessing'
    SGX_STOCK_CODE_SCRAPING = '/home/is3107/airflow/dags/sourcefiles/sgx/stock_list'
    GCP_FUNCTIONS_BASE_URL = 'https://us-west1-is3107.cloudfunctions.net/'
    SGX_STOCK_PRICES = '/home/is3107/airflow/dags/sourcefiles/sgx/stock_price'
    FOREX_TO_SGD = '/home/is3107/airflow/dags/sourcefiles/forex'
    SGX_STOCK_CHART_1D = '/home/is3107/airflow/dags/sourcefiles/sgx/stock_chart/1d'

