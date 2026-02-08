CREATE OR REPLACE TABLE `motwot_v2.daily_counts` AS
SELECT
  DATE(t.completedDate) AS test_date,
  t.testResult AS result_type,
  COUNT(*) AS count
FROM `motwot_v2.motwot_v2`, UNNEST(motTests) AS t
WHERE t.testResult IN ('PASSED', 'FAILED') -- Filter to just the two main types
  AND t.completedDate IS NOT NULL
GROUP BY 1, 2


