import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from docker.types import Mount
from airflow.operators.python import PythonOperator

from elt.load_csv import main

with DAG(
    dag_id="elt_dbt_pipeline",
    start_date=datetime(2026, 2, 7),
    schedule="@daily",
    catchup=False,
) as dag:

    elt_task = PythonOperator(
        task_id="elt_load_csv",
        python_callable=main,
    )
    
    custom_model_task = DockerOperator(
        task_id= "dbt_run",
        image= "custom-model-dbt:latest",
        command= "run --project-dir /opt/dbt --profiles-dir /root/.dbt",
        auto_remove= True,
        docker_url= "unix://var/run/docker.sock",
        network_mode= "coraline-assignment_elt_network",
        mount_tmp_dir= False,
        environment= {
            "DBT_DB_TYPE": os.getenv("DBT_DB_TYPE"),
            "DATA_WAREHOUSE_POSTGRES_HOST": os.getenv("DATA_WAREHOUSE_POSTGRES_HOST"),
            "DATA_WAREHOUSE_POSTGRES_DB": os.getenv("DATA_WAREHOUSE_POSTGRES_DB"),
            "DATA_WAREHOUSE_POSTGRES_PORT": os.getenv("DATA_WAREHOUSE_POSTGRES_PORT"),

            "DATA_WAREHOUSE_POSTGRES_USER": os.getenv("DATA_WAREHOUSE_POSTGRES_USER"),
            "DATA_WAREHOUSE_POSTGRES_PASSWORD": os.getenv("DATA_WAREHOUSE_POSTGRES_PASSWORD"),
            "DBT_DB_SCHEMA": os.getenv("DBT_DB_SCHEMA"),
            "DBT_THREAD": os.getenv("DBT_THREAD"),
        }
    )


    elt_task >> custom_model_task
    
