WITH temp AS (
SELECT c.p_date, a.ticker,
ROUND(AVG(a.NLTK_score),5) AS avg_twitter_score_ticker, ROUND(AVG(b.NLTK_score),5) AS avg_news_score_ticker, 
CASE
    WHEN AVG(a.NLTK_score) IS NULL THEN ROUND(AVG(b.NLTK_score),5)
    WHEN AVG(b.NLTK_score) IS NULL THEN ROUND(AVG(a.NLTK_score),5)
    ELSE ROUND((AVG(a.NLTK_score) + AVG(b.NLTK_score))/2, 5)
END AS combined_sentiment_score_ticker,
ROUND(AVG(c.NLTK_SIA_compound_score),5) AS benchmark_score_for_today_market
FROM `is3107.lti_dim.dim_twitter_sentiments_with_nltk` a
FULL OUTER JOIN `is3107.lti_dim.dim_financial_news_with_nltk` b ON a.p_date = b.p_date AND a.ticker = b.ticker
FULL OUTER JOIN `is3107.lti_dim.dim_reddit_with_nltk` c ON a.p_date = c.p_date
WHERE c.p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
GROUP BY c.p_date, a.ticker
)

SELECT t.*, 
s.company_name,
CASE
    WHEN t.combined_sentiment_score_ticker > 0.3 THEN 'POSITIVE'
    WHEN t.combined_sentiment_score_ticker < -0.3 THEN 'NEGATIVE'
    ELSE 'NEUTRAL'
END AS overall_sentiment_for_ticker
FROM temp t
LEFT JOIN `is3107.lti_ods.ods_sgx_stock_list_daily` s ON t.ticker = s.stock_code
WHERE s.p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"
