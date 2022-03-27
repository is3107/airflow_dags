from template import default_body


class Body():
    FINVIZ_STOCK_PERF = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_finviz_stock_performance_daily",
        "description": "getting finviz stock performance",
        "entryPoint": "pull_finviz_data"
    })
    FINVIZ_STOCK_OVW = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_finviz_stock_overview_daily",
        "description": "getting finviz stock overview",
        "entryPoint": "pull_finviz_data",
    })
    FINVIZ_STOCK_FIN = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_finviz_stock_financial_daily",
        "description": "getting finviz stock financials",
        "entryPoint": "pull_finviz_data",
    })
    SIMULATE_INTERNAL_TXN = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_internal_analyst_txn_daily",
        "description": "simulating internal analysts txn",
        "entryPoint": "simulate_txn",
    })
    YFINANCE_STOCK_FINANCIALS = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_yfinance_stock_financials_daily",
        "description": "ingest yfinance stocks financials",
        "entryPoint": "pull_yfinance_data",
    })
    YFINANCE_STOCK_FINANCIALS_DIM = default_body({
        "name": "projects/is3107/locations/us-west1/functions/dim_yfinance_stock_financials_daily",
        "description": "dimensioning yfinance stocks financials",
        "entryPoint": "dim_yfinance_data",
    })
    YFINANCE_STOCK_PRICES = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_yfinance_stock_prices_daily",
        "description": "ingest yfinance stocks prices",
        "entryPoint": "pull_yfinance_data",
    })
    REDDIT_SCRAPING = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_reddit_scraping_results_raw",
        "description": "reddit and twitter sentiments scraping results",
        "entryPoint": "scrape_text_data",
        "runtime": "python38",
    })
    REDDIT_PREPROCESSING = default_body({
        "name": "projects/is3107/locations/us-west1/functions/dim_reddit_with_nltk",
        "description": "process reddit and twitter sentiments with nltk library",
        "entryPoint": "preprocessing",
        "runtime": "python37",
        "availableMemoryMb": 512,
    })

