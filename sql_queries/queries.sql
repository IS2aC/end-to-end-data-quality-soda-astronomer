-- DATABASE NYC
CREATE DATABASE NYC;

-- ALL NYC SCHEMAS
-- SCHEMA BRONZE
CREATE SCHEMA NYC.NYC_SCHEMA
------------------------------------------- Bronze's table
------------------------------------------- Silver's table
--DIMENSIONS
-- dim_passenger_count
SELECT 
    ROW_NUMBER() OVER (ORDER BY passenger_count) AS passenger_count_id,
    passenger_count
FROM (
    SELECT DISTINCT passenger_count
    FROM yellow
    WHERE passenger_count IS NOT NULL
    UNION
    SELECT DISTINCT passenger_count
    FROM green
    WHERE passenger_count IS NOT NULL
) AS combined_table
ORDER BY passenger_count;


-- dim_taxi
SELECT 
    ROW_NUMBER() OVER (ORDER BY taxi) AS taxi_id, 
    taxi
FROM (
    SELECT DISTINCT taxi
    FROM green
    UNION
    SELECT DISTINCT taxi
    FROM yellow
) AS combined_table
ORDER BY taxi;


-- dim_date
SELECT 
    ROW_NUMBER() OVER (ORDER BY CAST(date AS DATE)) AS date_id,  -- Unique ID for each row
    CAST(date AS DATE) AS full_date,                            -- Convert the date column to DATE type
    EXTRACT(MONTH FROM CAST(date AS DATE)) AS month,            -- Extract the month from the date
    EXTRACT(DAY FROM CAST(date AS DATE)) AS day,                -- Extract the day of the month from the date
    CASE 
        WHEN EXTRACT(DOW FROM CAST(date AS DATE)) = 0 THEN 'Sunday'
        WHEN EXTRACT(DOW FROM CAST(date AS DATE)) = 1 THEN 'Monday'
        WHEN EXTRACT(DOW FROM CAST(date AS DATE)) = 2 THEN 'Tuesday'
        WHEN EXTRACT(DOW FROM CAST(date AS DATE)) = 3 THEN 'Wednesday'
        WHEN EXTRACT(DOW FROM CAST(date AS DATE)) = 4 THEN 'Thursday'
        WHEN EXTRACT(DOW FROM CAST(date AS DATE)) = 5 THEN 'Friday'
        WHEN EXTRACT(DOW FROM CAST(date AS DATE)) = 6 THEN 'Saturday'
    END AS dayofweek                                           -- Assign day names based on the day of the week
FROM (
    SELECT DISTINCT date 
    FROM (
        SELECT DISTINCT date
        FROM green
        UNION
        SELECT DISTINCT date
        FROM yellow
    ) AS date_table
) AS final_table;



--dim payment_type
SELECT DISTINCT 
    payment_type,
    CASE 
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge (free ride)'
        WHEN payment_type = 4 THEN 'Dispute (payment issue)'
        WHEN payment_type = 5 THEN 'Account'
        WHEN payment_type = 6 THEN 'Prepaid'
        WHEN payment_type = 0 THEN 'Unknown or Fraud'
        ELSE 'Unknown'
    END AS name
FROM (
    SELECT DISTINCT payment_type FROM yellow
    UNION 
    SELECT DISTINCT payment_type FROM green 
) AS p
WHERE payment_type IS NOT NULL
ORDER BY payment_type;

-- dim_location 
SELECT 
    DISTINCT DOLOCATIONID AS LocationID,
    -- Ajouter des noms descriptifs pour les zones
    CASE 
        WHEN DOLOCATIONID BETWEEN 1 AND 100 THEN 'Downtown Manhattan'
        WHEN DOLOCATIONID BETWEEN 101 AND 200 THEN 'Brooklyn Residential'
        WHEN DOLOCATIONID BETWEEN 201 AND 250 THEN 'Queens Commercial'
        WHEN DOLOCATIONID BETWEEN 251 AND 300 THEN 'Bronx Suburban'
        ELSE 'Other Zone'
    END AS ZoneName,
    -- Identifier les arrondissements
    CASE 
        WHEN DOLOCATIONID BETWEEN 1 AND 100 THEN 'Manhattan'
        WHEN DOLOCATIONID BETWEEN 101 AND 200 THEN 'Brooklyn'
        WHEN DOLOCATIONID BETWEEN 201 AND 250 THEN 'Queens'
        WHEN DOLOCATIONID BETWEEN 251 AND 300 THEN 'Bronx'
        ELSE 'Unknown'
    END AS Borough,
    -- Définir les types de zones (ServiceZone)
    CASE 
        WHEN DOLOCATIONID IN (1, 50, 100) THEN 'Airport'
        WHEN DOLOCATIONID BETWEEN 1 AND 100 THEN 'Business'
        WHEN DOLOCATIONID BETWEEN 101 AND 200 THEN 'Residential'
        WHEN DOLOCATIONID BETWEEN 201 AND 250 THEN 'Commercial'
        ELSE 'Other'
    END AS ServiceZone,
    -- Ajouter une note d'affluence basée sur des cas arbitraires
    CASE 
        WHEN DOLOCATIONID BETWEEN 1 AND 50 THEN 'High'
        WHEN DOLOCATIONID BETWEEN 51 AND 150 THEN 'Medium'
        WHEN DOLOCATIONID BETWEEN 151 AND 250 THEN 'Low'
        ELSE 'Unknown'
    END AS AffluenceRating
FROM (
    SELECT DOLOCATIONID FROM yellow
    UNION 
    SELECT DOLOCATIONID FROM green
    UNION 
    SELECT PULOCATIONID FROM yellow
    UNION 
    SELECT PULOCATIONID FROM green
) AS all_locations
ORDER BY DOLOCATIONID;

-- factable
WITH combined_taxi_data AS (
    SELECT 
        DATE, TAXI, 
        REPLACE(FARE_AMOUNT, ',', '.')::NUMERIC AS FARE_AMOUNT, 
        REPLACE(EXTRA, ',', '.')::NUMERIC AS EXTRA, 
        REPLACE(MTA_TAX, ',', '.')::NUMERIC AS MTA_TAX, 
        REPLACE(TIP_AMOUNT, ',', '.')::NUMERIC AS TIP_AMOUNT, 
        REPLACE(TOLLS_AMOUNT, ',', '.')::NUMERIC AS TOLLS_AMOUNT, 
        REPLACE(CONGESTION_SURCHARGE, ',', '.')::NUMERIC AS CONGESTION_SURCHARGE, 
        REPLACE(IMPROVEMENT_SURCHARGE, ',', '.')::NUMERIC AS IMPROVEMENT_SURCHARGE, 
        REPLACE(AIRPORT_FEE, ',', '.')::NUMERIC AS AIRPORT_FEE, 
        VENDORID, RATECODEID, DOLOCATIONID, PULOCATIONID, PAYMENT_TYPE, 
        TRIP_DISTANCE, PASSENGER_COUNT, STORE_AND_FWD_FLAG, 
        TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME
    FROM YELLOW
    UNION ALL
    SELECT 
        DATE, TAXI, 
        REPLACE(FARE_AMOUNT, ',', '.')::NUMERIC AS FARE_AMOUNT,
        REPLACE(EXTRA, ',', '.')::NUMERIC AS EXTRA,
        REPLACE(MTA_TAX, ',', '.')::NUMERIC AS MTA_TAX,
        REPLACE(TIP_AMOUNT, ',', '.')::NUMERIC AS TIP_AMOUNT,
        REPLACE(TOLLS_AMOUNT, ',', '.')::NUMERIC AS TOLLS_AMOUNT,
        REPLACE(CONGESTION_SURCHARGE, ',', '.')::NUMERIC AS CONGESTION_SURCHARGE,
        REPLACE(IMPROVEMENT_SURCHARGE, ',', '.')::NUMERIC AS IMPROVEMENT_SURCHARGE,
        REPLACE(AIRPORT_FEE, ',', '.')::NUMERIC AS AIRPORT_FEE,
        VENDORID, RATECODEID, DOLOCATIONID, PULOCATIONID, PAYMENT_TYPE, 
        TRIP_DISTANCE, PASSENGER_COUNT, STORE_AND_FWD_FLAG, 
        TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME
    FROM GREEN
)
SELECT 
    c.VENDORID AS vendor_id,
    c.RATECODEID AS ratecode_id,
    c.STORE_AND_FWD_FLAG,
    
    -- Clé étrangère vers la dimension date
    d.date_id, 

    -- Clé étrangère vers la dimension taxi
    t.taxi_id, 

    -- Clé étrangère vers la dimension location (pickup et dropoff)
    l1.locationid AS pickup_location_id,
    l2.locationid AS dropoff_location_id,

    -- Clé étrangère vers la dimension payment_type
    p.payment_type,

    -- Clé étrangère vers la dimension passenger_count
    pc.passenger_count_id,

    -- Mesures Fact Table
    c.TRIP_DISTANCE,
    c.TPEP_PICKUP_DATETIME AS pickup_datetime,
    c.TPEP_DROPOFF_DATETIME AS dropoff_datetime,
    c.FARE_AMOUNT,
    c.EXTRA,
    c.MTA_TAX,
    c.TIP_AMOUNT,
    c.TOLLS_AMOUNT,
    c.CONGESTION_SURCHARGE,
    c.IMPROVEMENT_SURCHARGE,
    c.AIRPORT_FEE,
    (c.FARE_AMOUNT + c.EXTRA + c.MTA_TAX + c.TIP_AMOUNT + c.TOLLS_AMOUNT + c.CONGESTION_SURCHARGE + c.IMPROVEMENT_SURCHARGE + c.AIRPORT_FEE) AS total_amount
FROM combined_taxi_data AS c
LEFT JOIN SILVER_DIM_DATE d ON c.DATE = d.full_date
LEFT JOIN SILVER_DIM_TAXI t ON c.TAXI = t.taxi
LEFT JOIN SILVER_DIM_LOCATION l1 ON c.PULOCATIONID = l1.locationid
LEFT JOIN SILVER_DIM_LOCATION l2 ON c.DOLOCATIONID = l2.locationid
LEFT JOIN SILVER_DIM_PAYMENT_TYPE p ON c.PAYMENT_TYPE = p.payment_type
LEFT JOIN SILVER_DIM_PASSENGER_COUNT pc ON c.PASSENGER_COUNT = pc.passenger_count
WHERE c.VENDORID IS NOT NULL
AND c.RATECODEID IS NOT NULL
AND c.STORE_AND_FWD_FLAG IS NOT NULL
AND c.TRIP_DISTANCE IS NOT NULL
AND c.TPEP_PICKUP_DATETIME IS NOT NULL
AND c.TPEP_DROPOFF_DATETIME IS NOT NULL
AND c.FARE_AMOUNT IS NOT NULL
AND c.EXTRA IS NOT NULL
AND c.MTA_TAX IS NOT NULL
AND c.TIP_AMOUNT IS NOT NULL
AND c.TOLLS_AMOUNT IS NOT NULL
AND c.CONGESTION_SURCHARGE IS NOT NULL
AND c.IMPROVEMENT_SURCHARGE IS NOT NULL
AND c.AIRPORT_FEE IS NOT NULL
AND d.date_id IS NOT NULL
AND t.taxi_id IS NOT NULL
AND l1.locationid IS NOT NULL
AND l2.locationid IS NOT NULL
AND p.payment_type IS NOT NULL
AND pc.passenger_count_id IS NOT NULL;





-------------------------------------------- Gold's table