-- 1
ALTER TABLE `motwot_v2.motwot_main_enriched`
ADD COLUMN last_test_date TIMESTAMP,
ADD COLUMN last_test_result STRING,
ADD COLUMN mileage INT64,
ADD COLUMN vehicle_age INT64,
ADD COLUMN pass_count INT64,
ADD COLUMN fail_count INT64;

-- 2
UPDATE `motwot_v2.motwot_main_enriched`
SET 
  last_test_date = motTests[SAFE_OFFSET(ARRAY_LENGTH(motTests) - 1)].completedDate,
  last_test_result = COALESCE(motTests[SAFE_OFFSET(ARRAY_LENGTH(motTests) - 1)].testResult, 'NEVER MOT'),
  mileage = CAST(motTests[SAFE_OFFSET(ARRAY_LENGTH(motTests) - 1)].odometerValue AS INT64),
  vehicle_age = DATE_DIFF(CURRENT_DATE(), DATE(firstUsedDate), YEAR),
  pass_count = (SELECT COUNT(*) FROM UNNEST(motTests) AS t WHERE t.testResult = 'PASSED'),
  fail_count = (SELECT COUNT(*) FROM UNNEST(motTests) AS t WHERE t.testResult = 'FAILED')
WHERE TRUE;