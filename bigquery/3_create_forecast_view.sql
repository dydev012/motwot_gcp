CREATE OR REPLACE VIEW `motwot_v2.prediction_output` AS
WITH combined_data AS (
  -- Historical Data
  SELECT 
    test_date, 
    CONCAT(result_type, ' - ACTUAL') AS chart_series,
    count
  FROM `motwot_v2.daily_counts`
  WHERE test_date < CURRENT_DATE() 

  UNION ALL

  -- Forecast Data
  SELECT 
    DATE(forecast_timestamp) AS test_date, 
    CONCAT(result_type, ' - FORECAST') AS chart_series, 
    forecast_value AS count
  FROM ML.FORECAST(MODEL `motwot_v2.pass_fail_forecast_model`, STRUCT(30 AS horizon))
  WHERE DATE(forecast_timestamp) >= CURRENT_DATE()
)

SELECT * FROM combined_data
WHERE test_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND test_date <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)