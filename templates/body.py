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
    ANALYST_PORTFOLIO_PERF = default_body({
        "name": "projects/is3107/locations/us-west1/functions/dwd_internal_analyst_portfolio_daily",
        "description": "calculating analysts' portfolio peformance",
        "entryPoint": "simulate_txn",
    })
    YFINANCE_STOCK_FINANCIALS = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_yfinance_stock_financials_daily",
        "description": "ingest yfinance stocks financials",
        "entryPoint": "pull_yfinance_data"
    })
    YFINANCE_STOCK_FINANCIALS_DWD = default_body({
        "name": "projects/is3107/locations/us-west1/functions/dwd_yfinance_stock_financials_daily",
        "description": "Data warehousing details for yfinance stocks financials",
        "entryPoint": "dwd_yfinance_data"
    })
    YFINANCE_STOCK_PRICES = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_yfinance_stock_prices_daily",
        "description": "ingest yfinance stocks prices",
        "entryPoint": "pull_yfinance_data",
        "timeout": "540s"
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
    NEWS_SCRAPING = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_ticker_level_financial_news_scraping_results",
        "description": "ticker level financial news scraping results",
        "entryPoint": "scrape_news",
        "timeout": "300s"
    })
    NEWS_PREPROCESSING = default_body({
        "name": "projects/is3107/locations/us-west1/functions/dim_financial_news_with_nltk",
        "description": "ticker level sentiments",
        "entryPoint": "preprocessing",
        "runtime": "python37",
        "availableMemoryMb": 512,
    })
    TWITTER_SCRAPING = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_ticker_level_twitter_posts",
        "description": "ticker level twitter posts scraping result",
        "entryPoint": "scrape_tweets",
        "runtime": "python38",
    })
    TWITTER_PREPROCESSING = default_body({
        "name": "projects/is3107/locations/us-west1/functions/dim_twitter_sentiments_with_nltk",
        "description": "ticker level twitter sentiments",
        "entryPoint": "preprocessing",
        "runtime": "python37",
        "timeout": "300s",
        "availableMemoryMb": 512,
    })
    SGX_STOCK_CODE_SCRAPING = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_sgx_stock_code_scraping",
        "description": "collect stock code",
        "entryPoint": "get_stock_list",
    })
    SGX_STOCK_PRICES = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_sgx_stock_prices",
        "description": "get stock prices from SGX",
        "entryPoint": "get_stock_price",
    })
    FOREX_TO_SGD = default_body({
        "name": "projects/is3107/locations/us-west1/functions/dwd_exchange_rates_daily",
        "description": "get daily exchange rate for conversion of various currencies to SGD",
        "entryPoint": "get_forex",
    })
    SGX_STOCK_CHART_1D = default_body({
        "name": "projects/is3107/locations/us-west1/functions/ods_sgx_chart_1d",
        "description": "Import stock prices from the 1D Chart",
        "entryPoint": "get_stock_prices",
        "timeout": "540s",
        "availableMemoryMb": 1024,
    })

