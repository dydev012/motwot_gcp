CREATE OR REPLACE TABLE `motwot_v2.daily_counts` AS
SELECT
  DATE(t.completedDate) AS test_date,
  t.testResult AS result_type,
  COUNT(*) AS count
FROM `motwot_v2.motwot_main`, UNNEST(motTests) AS t
WHERE t.testResult IN ('PASSED', 'FAILED')
  AND t.completedDate IS NOT NULL
GROUP BY 1, 2


