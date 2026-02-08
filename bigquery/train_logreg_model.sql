CREATE OR REPLACE MODEL `motwot_v2.pass_fail_logreg_model`
OPTIONS(
  model_type='LOGISTIC_REG',
  input_label_cols=['last_test_result'], -- This is what we want to predict
  auto_class_weights=TRUE 
) AS
SELECT
  last_test_result,
  mileage,
  vehicle_age,
  UPPER(make) AS make,
  UPPER(model) AS model,
  UPPER(primaryColour) AS primaryColour
FROM `motwot_v2.motwot_v2_enriched`
WHERE last_test_result IN ('PASSED', 'FAILED')
  AND mileage IS NOT NULL 
  AND vehicle_age IS NOT NULL