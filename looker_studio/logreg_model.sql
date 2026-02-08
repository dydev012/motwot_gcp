SELECT
  *,
  (SELECT prob FROM UNNEST(predicted_last_test_result_probs) WHERE label = 'PASSED') AS pass_probability
FROM ML.PREDICT(MODEL `motwot_v2.pass_fail_logreg_model`, 
  (SELECT 
    @param_mileage AS mileage,
    @param_vehicle_age AS vehicle_age,
    @param_make AS make,
    @param_model AS model,
    @param_primaryColour AS primaryColour
  )
)