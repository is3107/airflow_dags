SELECT p_date, ticker, company_name, combined_sentiment_score_ticker, benchmark_score_for_today_market, overall_sentiment_for_ticker 
FROM `is3107.lti_dwa.dwa_aggregated_daily_sentiments_data` 
WHERE p_date = "{{ dag_run.logical_date.astimezone(dag.timezone) | ds }}"