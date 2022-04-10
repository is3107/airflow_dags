WITH total_transactions AS (
    SELECT
        Analyst,
        Ticker,
        Amount,
        Txn_Type,
        Price,
        Txn_Time,
        p_date
    FROM 
        `is3107.lti_dwd.dwd_internal_analyst_txn_daily` 
    WHERE
        p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    UNION ALL
    SELECT
        Analyst,
        Ticker,
        Amount,
        'BUY' AS Txn_Type,
        Avg_Price AS Price,
        DATETIME(p_date) AS Txn_Time,
        p_date
    FROM
        `is3107.lti_dwa.dwa_all_portfolio_snapshot_daily`
    WHERE
        p_date = "{{ (dag_run.logical_date.astimezone(dag.timezone) - macros.timedelta(days=1)) | ds }}"
),

stock_amt AS (
    SELECT
        Analyst,
        Ticker,
        SUM(Amount) AS Amount,
        SUM(IF(p_date = "{{ (dag_run.logical_date.astimezone(dag.timezone) - macros.timedelta(days=1)) | ds }}", Amount, 0)) AS Daily_Amount_Change,
        DATE("{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}") AS Actual_p_date
    FROM 
        (
            SELECT
                Analyst,
                Ticker,
                IF(Txn_Type = 'BUY', Amount, -Amount) AS Amount,
                p_date
            FROM
                total_transactions
        )
    GROUP BY
        Analyst,
        Ticker
),
cum_stock AS (
    SELECT
        Analyst,
        Ticker,
        Txn_Type,
        Amount,
        Price,
        Txn_Time,
        SUM(IF(Txn_Type = 'BUY', Amount, -Amount)) OVER(PARTITION BY Analyst, Ticker ORDER BY Txn_Time) AS Running_Amount,
        SUM(IF(Txn_Type = 'BUY', Amount, -Amount) * Price) OVER(PARTITION BY Analyst, Ticker ORDER BY Txn_Time) AS Running_Value
    FROM
        total_transactions
),
stock_realised_gain AS (
    SELECT
        Analyst,
        Ticker,
        SUM(IF(Txn_Type = 'SELL', Amount * (Price - (Running_Value/Running_Amount)), 0)) AS Realised_Gain
    FROM
        cum_stock
    GROUP BY 
        Analyst,
        Ticker
),
stock_unrealised_gain AS (
    SELECT
        Analyst,
        Ticker,
        Avg_Price,
        Avg_Price - Actual_Price As Unrealised_Price_Diff
    FROM (
        SELECT
            Analyst,
            Ticker,
            Running_Avg_Price AS Avg_Price
        FROM (
            SELECT
                Analyst,
                Ticker,
                Txn_Type,
                Amount,
                Running_Amount/Running_Value AS Running_Avg_Price,
                ROW_NUMBER() OVER(PARTITION BY Analyst, Ticker ORDER BY Txn_Time DESC) AS Num
            FROM
                cum_stock
        )
        WHERE
            Num = 1
    ) avg_prices
    LEFT JOIN
        (
            -- Since price table is not filled in for non-trading days, take past 1 week and get latest price from that
            SELECT
                Stock_Code,
                Actual_Price
            FROM (
                SELECT
                    Stock_Code,
                    Close AS Actual_Price,
                    p_date,
                    MAX(p_date) AS Latest_p_date
                FROM
                    `is3107.lti_dwd.dwd_stock_prices_daily`
                WHERE
                    p_date BETWEEN "{{ (dag_run.logical_date.astimezone(dag.timezone) - macros.timedelta(days=7)) | ds }}" AND "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
                GROUP BY
                    Stock_Code,
                    Close,
                    p_date
            )
            WHERE
                p_date = Latest_p_date
        ) actual_prices
    ON
        avg_prices.Ticker = actual_prices.Stock_Code
)


SELECT
    stock_amt.Analyst,
    stock_amt.Ticker,
    Amount,
    Daily_Amount_Change,
    Realised_Gain,
    Unrealised_Price_Diff * Amount AS Unrealised_Gain,
    Actual_p_date AS p_date,
    Avg_Price
FROM
    stock_amt
LEFT JOIN
    stock_realised_gain
ON
    stock_amt.Analyst = stock_realised_gain.Analyst
AND
    stock_amt.Ticker = stock_realised_gain.Ticker
LEFT JOIN
    stock_unrealised_gain
ON
    stock_amt.Analyst = stock_unrealised_gain.Analyst
AND
    stock_amt.Ticker = stock_unrealised_gain.Ticker



