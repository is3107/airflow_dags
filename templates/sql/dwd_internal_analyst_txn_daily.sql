CREATE TEMP FUNCTION check_valid(arr ARRAY<FLOAT64>)
RETURNS BOOL 
LANGUAGE js AS r"""
total = 0;
isValid = true;
for (var i = 0; i < arr.length; i++) {
    new_total = total + arr[i];
    if (new_total >= 0) {
        total = new_total;
    }

    if (i == arr.length - 1 && new_total < 0) {
        isValid = false;
    }
}

return isValid;

""";


WITH all_transactions AS (
SELECT
    Analyst,
    Ticker,
    Price,
    Txn_Type,
    IF(Txn_Type = 'BUY', Amount, -Amount) AS Amount,
    Txn_Time,
    p_date
FROM
    `is3107.lti_ods.ods_internal_analyst_txn_daily`
WHERE
    p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"

UNION ALL

SELECT
    Analyst,
    Ticker,
    0 AS Price,
    'Buy' AS Txn_Type,
    Amount,
    "{{ macros.datetime.min }}" AS Txn_Time,
    p_date
FROM
    `is3107.lti_dwa.dwa_all_portfolio_snapshot_daily`
WHERE
    p_date = "{{ (dag_run.logical_date.astimezone(dag.timezone) - macros.timedelta(days=1)) | ds }}"

),

labelled_transactions AS (
SELECT
    Analyst,
    Ticker,
    Price,
    Txn_Type,
    Amount,
    Txn_Time,
    check_valid(array_agg(Amount) over(PARTITION BY Analyst, Ticker ORDER BY Txn_Time, Txn_Type)) AS Is_Valid,
    p_date
FROM
    all_transactions

)

SELECT
    Analyst,
    Ticker,
    Price,
    Txn_Type,
    Amount,
    p_date,
    Txn_Time
FROM
    labelled_transactions
WHERE
    Is_Valid = true
AND
    p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    




