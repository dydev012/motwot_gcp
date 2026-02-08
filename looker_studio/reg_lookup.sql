WITH vehicle_data AS (
  SELECT 
    mileage,
    vehicle_age,
    make,
    model,
    primaryColour
  FROM `motwot.motwot_v2.motwot_v2_enriched`
  WHERE registration = @reg_input
  LIMIT 1
)

SELECT
  p.*,
  (SELECT prob FROM UNNEST(p.predicted_last_test_result_probs) WHERE label = 'PASSED') AS pass_probability
FROM vehicle_data vd,
ML.PREDICT(
  MODEL `motwot_v2.pass_fail_logreg_model`,
  (SELECT * FROM vehicle_data)
) p