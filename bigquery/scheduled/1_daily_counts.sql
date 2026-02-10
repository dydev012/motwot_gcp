CREATE OR REPLACE TABLE `motwot_v2.daily_counts`
PARTITION BY DATE_TRUNC(test_date, MONTH)
AS
SELECT
  last_test_date AS test_date,
  last_test_result AS result_type,
  COUNT(*) AS count
FROM `motwot_v2.motwot_main_enriched`
WHERE 
  DATE(last_test_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
  AND last_test_result IN ('PASSED', 'FAILED')
GROUP BY 1, 2