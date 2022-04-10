WITH stock_financials_today AS (
    SELECT
        *
    FROM
        `is3107.lti_dwd.dwd_yfinance_stock_financials_daily` stock_financials
    LEFT JOIN (
        SELECT
            Stock_Code
        FROM
            `is3107.lti_dwd.dwd_stock_prices_daily`
        WHERE
            p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
    ) stock_prices
    ON
        stock_financials.Ticker = CONCAT(stock_prices.Stock_Code, '.SI')
    WHERE
        stock_financials.p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
),
stock_prices_today AS (
    SELECT
        *
    FROM
        `is3107.lti_dwd.dwd_stock_prices_daily` stock_prices    
    WHERE
        stock_prices.p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
)

SELECT 
    COALESCE(stock_financials.p_date, stock_prices.p_date) AS p_date,
    COALESCE(stock_prices.Stock_Code, stock_financials.Ticker) AS Stock_Code,
    COALESCE(stock_prices.Stock_Name, stock_financials.Ticker) AS Stock_Name, -- As y_finance table does not take in company name, substitute with Ticker if needed
    Open,
    Close,
    High,
    Low,
    Volume,
    Market_cap__intra_day_,
    Enterprise_value,
    Trailing_PE,
    Forward_PE,
    PEG_Ratio__5_yr_expected_,
    Pricesales__ttm_,
    Pricebook__mrq_
    Enterprise_valuerevenue,
    Enterprise_valueEBITDA,
    Beta__5Y_monthly_,
    _52_week_change_3,
    SnP_500__52_week_change_3,
    _52_week_high_3,
    _52_week_low_3,
    _50_day_moving_average_3,
    _200_day_moving_average_3,
    Avg_vol__3_month__3,
    Avg_vol__10_day__3,
    Shares_outstanding_5,
    Implied_shares_outstanding_6,
    Float_8,
    percent_held_by_insiders_1,
    percent_held_by_institutions_1,
    Shares_short_4,
    Short_ratio_4,
    Short_percent_of_float_4,
    Short_percent_of_shares_outstanding_4,
    Shares_short__prior_month___4,
    Forward_annual_dividend_rate_4,
    Forward_annual_dividend_yield_4,
    Trailing_annual_dividend_rate_3,
    Trailing_annual_dividend_yield_3,
    _5_year_average_dividend_yield_4,
    Payout_ratio_4,
    Last_split_factor_2,
    Profit_margin,
    Operating_margin__ttm_,
    Return_on_assets__ttm_,
    Return_on_equity__ttm_,
    Revenue__ttm_,
    Revenue_per_share__ttm_,
    Quarterly_revenue_growth__yoy_,
    Gross_profit__ttm_,
    EBITDA,
    Net_income_avi_to_common__ttm_,
    Diluted_EPS__ttm_,
    Quarterly_earnings_growth__yoy_,
    Total_cash__mrq_,
    Total_cash_per_share__mrq_,
    Total_debt__mrq_,
    Total_debtequity__mrq_,
    Current_ratio__mrq_,
    Book_value_per_share__mrq_,
    Operating_cash_flow__ttm_,
    Levered_free_cash_flow__ttm_
FROM
    stock_financials_today stock_financials
FULL OUTER JOIN
    stock_prices_today stock_prices
ON
    stock_financials.Ticker = CONCAT(stock_prices.Stock_Code, '.SI')
    