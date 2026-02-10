SELECT
  * EXCEPT(predicted_last_test_result_probs), 
  (SELECT prob 
   FROM UNNEST(predicted_last_test_result_probs) 
   WHERE label = 'PASSED') AS pass_probability

FROM ML.PREDICT(
  MODEL `motwot_v2.pass_fail_logreg_model`,
  (
    SELECT
    registration, 
    mileage,
    vehicle_age,
    make,
    model,
    primaryColour
    FROM `motwot_v2.motwot_main_enriched`
    WHERE registration = @reg_input
    LIMIT 1
  )
)