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
SELECT * FROM `motwot_v2.daily_counts`