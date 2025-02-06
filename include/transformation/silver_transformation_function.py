from astro import sql as aql
from astro.sql.table import Table


""" All these functions we'll use astro.sql methods for create silver's table on our data storage space """
""" SILVER'S TABLES ARE  : silver_tableName """

##################################### DIMENSIONS #####################################
@aql.transform()
def dim_passenger_count(yellow_table: Table, green_table :  Table):

    return """
    SELECT 
        ROW_NUMBER() OVER (ORDER BY passenger_count) AS passenger_count_id,
        passenger_count
    FROM (
        SELECT DISTINCT passenger_count
        FROM {{ yellow_table }}
        WHERE passenger_count IS NOT NULL
        UNION
        SELECT DISTINCT passenger_count
        FROM {{ green_table }}
        WHERE passenger_count IS NOT NULL
    ) AS combined_table
    ORDER BY passenger_count
    """

@aql.transform()
def dim_taxi(yellow_table: Table, green_table :  Table):

    return """
    SELECT 
        ROW_NUMBER() OVER (ORDER BY taxi) AS taxi_id, 
        taxi
    FROM (
        SELECT DISTINCT taxi
        FROM  {{ yellow_table }}
        UNION
        SELECT DISTINCT taxi
        FROM {{ green_table }}
    ) AS combined_table
    ORDER BY taxi
    """

@aql.transform()
def dim_date(yellow_table: Table, green_table :  Table):
    return """
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
            FROM {{ yellow_table }}
            WHERE date IS NOT NULL
            UNION
            SELECT DISTINCT date
            FROM {{ green_table }}
            WHERE date IS NOT NULL
        ) AS date_table
    ) AS final_table
    WHERE date IS NOT NULL;
    """

@aql.transform()
def dim_payment_type(yellow_table: Table, green_table :  Table):
    return """
    SELECT DISTINCT payment_type,
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
        SELECT DISTINCT payment_type FROM {{yellow_table}}
        UNION 
        SELECT DISTINCT payment_type FROM {{green_table}}
    ) AS p
    WHERE payment_type IS NOT NULL
    ORDER BY payment_type;
    """


@aql.transform()
def dim_location(yellow_table: Table, green_table :  Table):
    return """
    WITH all_locations AS (
        SELECT DOLOCATIONID AS LocationID FROM {{yellow_table}} WHERE DOLOCATIONID IS NOT NULL
        UNION 
        SELECT DOLOCATIONID FROM {{green_table}} WHERE DOLOCATIONID IS NOT NULL
        UNION 
        SELECT PULOCATIONID FROM {{yellow_table}} WHERE PULOCATIONID IS NOT NULL
        UNION 
        SELECT PULOCATIONID FROM {{green_table}} WHERE PULOCATIONID IS NOT NULL
    )
    
    SELECT DISTINCT LocationID,
        -- Ajouter des noms descriptifs pour les zones
        CASE 
            WHEN LocationID BETWEEN 1 AND 100 THEN 'Downtown Manhattan'
            WHEN LocationID BETWEEN 101 AND 200 THEN 'Brooklyn Residential'
            WHEN LocationID BETWEEN 201 AND 250 THEN 'Queens Commercial'
            WHEN LocationID BETWEEN 251 AND 300 THEN 'Bronx Suburban'
            ELSE 'Other Zone'
        END AS ZoneName,
        -- Identifier les arrondissements
        CASE 
            WHEN LocationID BETWEEN 1 AND 100 THEN 'Manhattan'
            WHEN LocationID BETWEEN 101 AND 200 THEN 'Brooklyn'
            WHEN LocationID BETWEEN 201 AND 250 THEN 'Queens'
            WHEN LocationID BETWEEN 251 AND 300 THEN 'Bronx'
            ELSE 'Unknown'
        END AS Borough,
        -- Définir les types de zones (ServiceZone)
        CASE 
            WHEN LocationID IN (1, 50, 100) THEN 'Airport'
            WHEN LocationID BETWEEN 1 AND 100 THEN 'Business'
            WHEN LocationID BETWEEN 101 AND 200 THEN 'Residential'
            WHEN LocationID BETWEEN 201 AND 250 THEN 'Commercial'
            ELSE 'Other'
        END AS ServiceZone,
        -- Ajouter une note d'affluence basée sur des cas arbitraires
        CASE 
            WHEN LocationID BETWEEN 1 AND 50 THEN 'High'
            WHEN LocationID BETWEEN 51 AND 150 THEN 'Medium'
            WHEN LocationID BETWEEN 151 AND 250 THEN 'Low'
            ELSE 'Unknown'
        END AS AffluenceRating
    FROM all_locations
    ORDER BY LocationID;
    """

@aql.transform()
def fact_table_taxi_trips(yellow_table: Table, green_table: Table, dim_date: Table, dim_taxi: Table, 
                    dim_location: Table, dim_payment_type: Table, dim_passenger_count: Table):
    return """
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
        FROM {{ yellow_table }}
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
        FROM {{ green_table }}
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
    LEFT JOIN {{ dim_date }} d ON c.DATE = d.full_date
    LEFT JOIN {{ dim_taxi }} t ON c.TAXI = t.taxi
    LEFT JOIN {{ dim_location }} l1 ON c.PULOCATIONID = l1.locationid
    LEFT JOIN {{ dim_location }} l2 ON c.DOLOCATIONID = l2.locationid
    LEFT JOIN {{ dim_payment_type }} p ON c.PAYMENT_TYPE = p.payment_type
    LEFT JOIN {{ dim_passenger_count }} pc ON c.PASSENGER_COUNT = pc.passenger_count
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
    """