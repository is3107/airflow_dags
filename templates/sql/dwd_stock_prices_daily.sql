WITH y_finance_stock_prices_today AS (
    SELECT
        y_finance.p_date,
        Ticker,
        Open * Rate AS Open,
        High * Rate AS High,
        Low * Rate AS Low,
        Close * Rate AS Close,
        Volume
    FROM
        `is3107.lti_ods.ods_yfinance_stock_prices_daily` y_finance
    LEFT JOIN (
        SELECT
            Currency,
            Stock_Code
        FROM
            `is3107.lti_ods.ods_sgx_stock_price_daily`
        WHERE
            p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    ) sgx
    ON
        y_finance.Ticker = sgx.Stock_Code
    LEFT JOIN `is3107.lti_dwd.dwd_exchange_rates_daily` forex
    ON
        y_finance.p_date = forex.p_date
    AND
        sgx.Currency = forex.Currency
    WHERE
        y_finance.p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"

),
sgx_stock_prices_today AS (
    -- USD denominated tickers
    SELECT
        sgx.p_date,
        Stock_Code,
        Stock_Name,
        IF(Volume = 0, Last * Rate, Open * Rate) AS Open,
        IF(Volume = 0, Last * Rate, High * Rate) AS High,
        IF(Volume = 0, Last * Rate, Low * Rate) AS Low,
        Last * Rate AS Last,
        Volume
    FROM
        `is3107.lti_ods.ods_sgx_stock_price_daily` sgx
    LEFT JOIN `is3107.lti_dwd.dwd_exchange_rates_daily` forex
    ON
        sgx.p_date = forex.p_date
    AND
        sgx.Currency = forex.Currency
    WHERE
        sgx.p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"

)


SELECT 
    COALESCE(sgx.p_date, y_finance.p_date) AS p_date,
    COALESCE(sgx.Stock_Code, y_finance.Ticker) AS Stock_Code,
    COALESCE(sgx.Stock_Name, y_finance.Ticker) AS Stock_Name,             -- As y_finance table does not take in company name, substitute with Ticker if needed
    COALESCE(sgx.Open, y_finance.Open) AS Open,
    COALESCE(sgx.Last, y_finance.Close) AS Close,
    COALESCE(sgx.High, y_finance.High) AS High,
    COALESCE(sgx.Low, y_finance.Low) AS Low,
    COALESCE(sgx.Volume, y_finance.Volume) AS Volume
FROM
    sgx_stock_prices_today sgx
FULL OUTER JOIN
    y_finance_stock_prices_today y_finance
ON
    sgx.Stock_Code = y_finance.Ticker



