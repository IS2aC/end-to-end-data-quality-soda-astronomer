from astro import sql as aql
from astro.sql.table import Table


""" GOLD'S TABLES ARE  : silver_tableName """

@aql.transform()
def gold_general_kpi(fact_table: Table, dim_taxi: Table, dim_date: Table):
    return """
    SELECT dim_date.full_date,  dim_taxi.taxi, ft.fare_amount, ft.total_amount, ft.trip_distance, ft.congestion_surcharge, ft.improvement_surcharge
    FROM {{fact_table}} as ft, {{dim_taxi}} as dim_taxi, {{dim_date}} as dim_date
    WHERE ft.date_id = dim_date.date_id and ft.taxi_id = dim_taxi.taxi_id
    """


@aql.transform()
def gold_map_analysis(fact_table: Table, dim_taxi: Table, dim_date: Table, dim_location: Table):
    return """
    SELECT dt.full_date,  dim_taxi.taxi, ft.trip_distance, ft.pickup_location_id, ft.dropoff_location_id, dl.zonename, dl.borough, dl.affluencerating
    FROM {{fact_table}} as ft , {{dim_date}} as dt, {{dim_location}} as dl, {{dim_taxi}} as dim_taxi
    WHERE ft.date_id = dt.date_id and ft.pickup_location_id = dl.locationid
    """
