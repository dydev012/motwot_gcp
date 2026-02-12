CREATE OR REPLACE MODEL `motwot_v2.pass_fail_forecast_model`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'test_date',
  time_series_data_col = 'count',
  time_series_id_col = 'result_type', 
  holiday_region = 'GB', 
  auto_arima = TRUE,
  data_frequency = 'DAILY',
  clean_spikes_and_dips = TRUE
) AS
SELECT 
    DATE(test_date) AS test_date, 
    result_type,
    SUM(count) AS count
FROM `motwot_v2.daily_counts`
WHERE DATE(test_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
GROUP BY 1, 2

-- evalute
-- SELECT * FROM ML.ARIMA_EVALUATE(MODEL `motwot_v2.pass_fail_forecast_model`) 