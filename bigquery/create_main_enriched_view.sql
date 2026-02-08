CREATE OR REPLACE VIEW `motwot_v2.motwot_v2_enriched` AS
SELECT 
  *,
  ARRAY_LAST(motTests).completedDate AS last_test_date,
  ARRAY_LAST(motTests).testResult AS last_test_result,
  CAST(ARRAY_LAST(motTests).odometerValue AS INT64) AS mileage,
  DATE_DIFF(CURRENT_DATE(), DATE(firstUsedDate), YEAR) AS vehicle_age,
  (SELECT COUNT(*) FROM UNNEST(motTests) AS t WHERE t.testResult = 'PASSED') AS pass_count,
  (SELECT COUNT(*) FROM UNNEST(motTests) AS t WHERE t.testResult = 'FAILED') AS fail_count
FROM `motwot_v2.motwot_v2`