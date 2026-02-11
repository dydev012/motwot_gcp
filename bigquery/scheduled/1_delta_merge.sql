MERGE `motwot.motwot_v2.motwot_main_enriched` AS main
USING (
    SELECT 
        *,
        motTests[SAFE_OFFSET(ARRAY_LENGTH(motTests) - 1)].completedDate AS last_test_date,
        COALESCE(motTests[SAFE_OFFSET(ARRAY_LENGTH(motTests) - 1)].testResult, 'NEVER MOT') AS last_test_result,
        CAST(motTests[SAFE_OFFSET(ARRAY_LENGTH(motTests) - 1)].odometerValue AS INT64) AS mileage,
        DATE_DIFF(CURRENT_DATE(), DATE(firstUsedDate), YEAR) AS vehicle_age,
        (SELECT COUNT(*) FROM UNNEST(motTests) AS t WHERE t.testResult = 'PASSED') AS pass_count,
        (SELECT COUNT(*) FROM UNNEST(motTests) AS t WHERE t.testResult = 'FAILED') AS fail_count
    FROM `motwot.motwot_v2.motwot_main_enriched_staging`
    WHERE modification IN ('CREATED', 'UPDATED', 'DELETED')
) AS delta
ON main.registration = delta.registration

-- Handle Deletions
WHEN MATCHED AND delta.modification = 'DELETED' THEN
    DELETE

-- Handle Updates
WHEN MATCHED AND delta.modification IN ('UPDATED', 'CREATED') THEN
    UPDATE SET 
        main.modification = delta.modification, 
        main.motTests = delta.motTests, 
        main.engineSize = delta.engineSize, 
        main.make = delta.make, 
        main.model = delta.model, 
        main.lastMotTestDate = delta.lastMotTestDate, 
        main.manufactureDate = delta.manufactureDate, 
        main.primaryColour = delta.primaryColour, 
        main.registrationDate = delta.registrationDate, 
        main.fuelType = delta.fuelType, 
        main.secondaryColour = delta.secondaryColour, 
        main.firstUsedDate = delta.firstUsedDate,
        main.last_test_date = delta.last_test_date,
        main.last_test_result = delta.last_test_result,
        main.mileage = delta.mileage,
        main.vehicle_age = delta.vehicle_age,
        main.pass_count = delta.pass_count,
        main.fail_count = delta.fail_count

-- Handle Inserts
WHEN NOT MATCHED AND delta.modification IN ('CREATED', 'UPDATED') THEN
    INSERT (
        registration, modification, motTests, engineSize, make, 
        model, lastMotTestDate, manufactureDate, primaryColour, 
        registrationDate, fuelType, secondaryColour, firstUsedDate,
        last_test_date, last_test_result, mileage, vehicle_age, pass_count, fail_count
    )
    VALUES (
        delta.registration, delta.modification, delta.motTests, delta.engineSize, delta.make, 
        delta.model, delta.lastMotTestDate, delta.manufactureDate, delta.primaryColour, 
        delta.registrationDate, delta.fuelType, delta.secondaryColour, delta.firstUsedDate,
        delta.last_test_date, delta.last_test_result, delta.mileage, delta.vehicle_age, delta.pass_count, delta.fail_count
    );