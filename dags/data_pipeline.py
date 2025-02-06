from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from astro import sql as aql
from astro.sql.table import Table
from include.transformation.silver_transformation_function import (dim_passenger_count, dim_taxi, dim_date, 
                                                                   dim_payment_type, dim_location)
from include.transformation.silver_transformation_function import fact_table_taxi_trips

from include.transformation.gold_transformation_functions import gold_general_kpi, gold_map_analysis
from datetime import datetime


@dag(
    start_date=datetime(2025, 1, 21),
    schedule_interval=None,
    catchup=False,
    tags=["data_pipeline"],
)
def pipeline():
    
    # Task for verification over bronze layer of our data pipeline
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python3.11')
    def check_over_bronze(scan_name='check_bronze'):
        from include.soda.check_function import check

        test = check(
            scan_name,
            file_checks_yml='bronze_checks.yml',
            configuration_file_yml='configuration.yml',
            data_source='snowflake',
            project_root='/usr/local/airflow/include',
        )
        return test

    # Routing logic for bronze check result
    def routing_bronze_logic(**kwargs):
        check_result = kwargs['ti'].xcom_pull(task_ids='check_over_bronze')
        if check_result.get("status") == "success":
            return "continue_silver"
        else:
            return "failed_bronze"

    routing_bronze = BranchPythonOperator(
        task_id="routing_bronze",
        python_callable=routing_bronze_logic,
        provide_context=True,  # Enable context passing
    )

    # Continue to silver task ##################################################################
    @task
    def continue_silver():
        print("Continue to silver --> let's start operation of transformation with astro sql.")

    # Task for failure case (empty operator for alerting)
    failed_bronze = EmptyOperator(task_id="failed_bronze")

    # Silver task for passenger count
    table_passenger_count = dim_passenger_count(
        yellow_table= Table( name  =  "YELLOW", conn_id =  "snowflake"),
        green_table= Table( name  =  "GREEN", conn_id =  "snowflake"),
        output_table = Table( name  =  "SILVER_DIM_PASSENGER_COUNT", conn_id =  "snowflake")
    )

    table_dim_taxi = dim_taxi(
        yellow_table= Table( name  =  "YELLOW", conn_id =  "snowflake"),
        green_table = Table( name  =  "GREEN", conn_id =  "snowflake"),
        output_table = Table( name  =  "SILVER_DIM_TAXI", conn_id =  "snowflake")
    )

    table_dim_date =  dim_date(
        yellow_table= Table( name  =  "YELLOW", conn_id =  "snowflake"),
        green_table = Table( name  =  "GREEN", conn_id =  "snowflake"),
        output_table=Table(name = 'SILVER_DIM_DATE', conn_id = "snowflake"),
    )

    table_dim_payment_type =  dim_payment_type(
        yellow_table= Table( name  =  "YELLOW", conn_id =  "snowflake"),
        green_table = Table( name  =  "GREEN", conn_id =  "snowflake"),
        output_table=Table(name = 'SILVER_DIM_PAYMENT_TYPE', conn_id = "snowflake"),
    )

    table_dim_location =  dim_location(
        yellow_table= Table( name  =  "YELLOW", conn_id =  "snowflake"),
        green_table = Table( name  =  "GREEN", conn_id =  "snowflake"),
        output_table=Table(name = 'SILVER_DIM_LOCATION', conn_id = "snowflake"),
    )


    table_facttable = fact_table_taxi_trips(
        yellow_table = Table( name  =  "YELLOW", conn_id =  "snowflake"),
        green_table = Table( name  =  "GREEN", conn_id =  "snowflake"),
        dim_date = Table(name = 'SILVER_DIM_DATE', conn_id = "snowflake"),
        dim_taxi = Table( name  =  "SILVER_DIM_TAXI", conn_id =  "snowflake"),
        dim_location = Table(name = 'SILVER_DIM_LOCATION', conn_id = "snowflake"),
        dim_passenger_count = Table( name  =  "SILVER_DIM_PASSENGER_COUNT", conn_id =  "snowflake"),
        dim_payment_type = Table(name = 'SILVER_DIM_PAYMENT_TYPE', conn_id = "snowflake"),
        output_table=Table(name = 'SILVER_FACT_TABLE', conn_id = "snowflake"),
    )

    # TEST OVER SILVERS TABLE #########################
    # Task for verification over bronze layer of our data pipeline
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python3.11')
    def check_over_silver(scan_name = 'check_silver'):
        from include.soda.check_function import check
        test = check(
            scan_name,
            file_checks_yml='silver_checks.yml',
            configuration_file_yml='configuration.yml',
            data_source='snowflake',
            project_root='/usr/local/airflow/include',
        )
        return test
    
    # Routing logic for bronze check result
    def routing_silver_logic(**kwargs):
        check_result = kwargs['ti'].xcom_pull(task_ids='check_over_silver')
        if check_result.get("status") == "success":
            return "continue_gold"
        else:
            return "failed_silver"
        
    routing_silver = BranchPythonOperator(
        task_id="routing_silver",
        python_callable=routing_silver_logic,
        provide_context=True,  # Enable context passing
    )
        
    @task
    def continue_gold():
        print("Continue to silver --> let's start operation of transformation with astro sql.")

    # Task for failure case (empty operator for alerting)
    failed_silver = EmptyOperator(task_id="failed_silver")

    # Continue to gold task ##################################################################
    ## task over gold table
    gold_general_kpi_task = gold_general_kpi(
        fact_table = Table(name = 'SILVER_FACT_TABLE', conn_id = 'snowflake'),
        dim_taxi = Table(name = 'SILVER_DIM_TAXI', conn_id = 'snowflake'),
        dim_date = Table(name = 'SILVER_DIM_DATE', conn_id = 'snowflake'),
        output_table = Table(name = 'GOLD_GENERAL_KPI', conn_id = 'snowflake')
    )

        ## task over gold table
    map_analysis_task = gold_map_analysis(
        fact_table = Table(name = 'SILVER_FACT_TABLE', conn_id = 'snowflake'),
        dim_taxi = Table(name = 'SILVER_DIM_TAXI', conn_id = 'snowflake'),
        dim_date = Table(name = 'SILVER_DIM_DATE', conn_id = 'snowflake'),
        dim_location = Table(name = 'SILVER_DIM_LOCATION', conn_id = 'snowflake'),
        output_table = Table(name = 'GOLD_MAP_ANALYSIS', conn_id = 'snowflake')
    )


    ################################ INSTANCIATION SPACE ##################################
    # Define the first part dependencies of our pipeline
    check_result = check_over_bronze()
    check_result_silver =  check_over_silver()
    
    # Routing logic follows check result
    routing_bronze.set_upstream(check_result)
    routing_silver.set_upstream(check_result_silver)
    
    # Continue to silver or handle failure case
    continue_silver_task = continue_silver()
    continue_gold_task =  continue_gold()
    routing_bronze >> [continue_silver_task, failed_bronze]
    routing_silver >> [continue_gold_task, failed_silver]
    
    # After silver step, move to table passenger count task
    continue_silver_task >> [table_passenger_count, table_dim_payment_type, 
                             table_dim_taxi, table_dim_date, table_dim_location] >> table_facttable
    
    table_facttable >> check_result_silver

    continue_gold_task >> [gold_general_kpi_task, map_analysis_task]
    
    


pipeline()
