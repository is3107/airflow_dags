"""
Paths class is a common ground to update all paths across DAGs
"""
class Paths():
    FINVIZ_STOCK_PERF = '/home/is3107/airflow/dags/sourcefiles/finviz/finviz_stock_perf'
    FINVIZ_STOCK_OVW = '/home/is3107/airflow/dags/sourcefiles/finviz/finviz_stock_ovw'
    FINVIZ_STOCK_FIN = '/home/is3107/airflow/dags/sourcefiles/finviz/finviz_stock_fin'
    SIMULATE_INTERNAL_TXN = '/home/is3107/airflow/dags/sourcefiles/analyst_internal/simulate_analyst_txn'
    GENERATE_REQ_SCRIPT = '/home/is3107/airflow/dags/templates/scripts/generate_requirements.sh'
    YFINANCE_STOCK_FINANCIALS = '/home/is3107/airflow/dags/sourcefiles/yfinance/yfinance_stock_financials'
    YFINANCE_STOCK_FINANCIALS_DIM = '/home/is3107/airflow/dags/sourcefiles/yfinance/yfinance_stock_financials_dim'
    YFINANCE_STOCK_PRICES = '/home/is3107/airflow/dags/sourcefiles/yfinance/yfinance_stock_prices'
    REDDIT_SCRAPING = '/home/is3107/airflow/dags/sourcefiles/reddit/scraping'
    REDDIT_PREPROCESSING = '/home/is3107/airflow/dags/sourcefiles/reddit/preprocessing'

