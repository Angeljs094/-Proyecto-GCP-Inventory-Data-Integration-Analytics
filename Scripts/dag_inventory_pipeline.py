# dag_inventory_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

# Importamos las variables y las funciones desde nuestros archivos modulares
import config
from etl_tasks import load_json_from_gcs_to_postgres, load_table_from_postgres_to_bigquery

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='gcs_json_to_postgres_load_final',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=['gcp', 'postgres', 'json', 'data-pipeline'],
) as dag:

    create_table_task = SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id=config.POSTGRES_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {config.POSTGRES_TABLE} (
            id_producto INT PRIMARY KEY,
            nombre_producto VARCHAR(255) NOT NULL,
            precio NUMERIC(10, 2) NOT NULL,
            fecha_registro DATE,
            en_stock BOOLEAN
        );
        """,
        autocommit=True,
    )

    load_data_task = PythonOperator(
        task_id='load_json_data_to_postgres',
        python_callable=load_json_from_gcs_to_postgres,
    )

    load_to_bq_task = PythonOperator(
        task_id='load_postgres_to_bigquery',
        python_callable=load_table_from_postgres_to_bigquery,
    )

    # Definición de dependencias
    create_table_task >> load_data_task >> load_to_bq_task