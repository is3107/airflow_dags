SELECT
    Analyst,
    First_Name,
    Last_Name,
    Email,
    Ticker,
    Amount,
    Daily_Amount_Change,
    Realised_Gain,
    Unrealised_Gain,
    p_date
FROM
    `is3107.lti_dwa.dwa_all_portfolio_snapshot_daily` portfolio
LEFT JOIN `is3107.internal.employee_details` employee
ON
    portfolio.Analyst = employee.Employee_Id
WHERE
    p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"