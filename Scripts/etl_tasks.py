# etl_tasks.py
import json
import logging
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
import config  # Importamos nuestras variables centralizadas

def load_json_from_gcs_to_postgres():
    logging.info(f"Iniciando descarga de {config.GCS_OBJECT} de bucket {config.GCS_BUCKET}")
    gcs_hook = GCSHook(gcp_conn_id=config.GCS_CONN_ID)
    file_data_bytes = gcs_hook.download(bucket_name=config.GCS_BUCKET, object_name=config.GCS_OBJECT)
    file_data = file_data_bytes.decode('utf-8')

    try:
        data_list = json.loads(file_data)
        if not isinstance(data_list, list):
            data_list = [data_list]
    except json.JSONDecodeError as e:
        logging.error(f"Error al decodificar el JSON: {e}")
        raise

    logging.info(f"Se cargaron {len(data_list)} registros del archivo JSON.")

    rows_to_insert = [
        (
            item.get('id_producto'),
            item.get('nombre_producto'),
            item.get('precio'),
            item.get('fecha_registro'),
            item.get('en_stock')
        )
        for item in data_list
    ]

    local_pg_hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    target_fields = ['id_producto', 'nombre_producto', 'precio', 'fecha_registro', 'en_stock']

    local_pg_hook.insert_rows(
        table=config.POSTGRES_TABLE,
        rows=rows_to_insert,
        target_fields=target_fields,
        commit_every=500
    )
    logging.info(f"Éxito: Inserción masiva completada en '{config.POSTGRES_TABLE}'.")


def load_table_from_postgres_to_bigquery():
    logging.info("Iniciando extracción desde Postgres")
    pg_hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID)
    sql = f"SELECT id_producto, nombre_producto, precio, fecha_registro, en_stock FROM {config.POSTGRES_TABLE};"

    try:
        df = pg_hook.get_pandas_df(sql)
    except Exception:
        logging.exception("Error al leer desde Postgres")
        raise

    if df.empty:
        logging.info("No hay filas para cargar. Terminando tarea.")
        return

    # Normalizaciones básicas
    if 'fecha_registro' in df.columns:
        df['fecha_registro'] = pd.to_datetime(df['fecha_registro'], errors='coerce').dt.date

    if 'en_stock' in df.columns:
        df['en_stock'] = df['en_stock'].map(
            lambda v: True if str(v).lower() in ('true', '1', 't', 'yes') 
            else (False if str(v).lower() in ('false', '0', 'f', 'no') else None)
        )

    # Preparar cliente BigQuery
    bq_hook = BigQueryHook(gcp_conn_id=config.BQ_CONN_ID, use_legacy_sql=False)
    client = bq_hook.get_client(project_id=config.BQ_PROJECT)
    destination_table = f"{config.BQ_PROJECT}.{config.BQ_DATASET}.{config.BQ_TABLE}"
    
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE 
        if config.BQ_WRITE_DISPOSITION == 'WRITE_TRUNCATE' 
        else bigquery.WriteDisposition.WRITE_APPEND
    )

    logging.info(f"Iniciando job de carga a BigQuery: {destination_table}")
    try:
        load_job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)
        load_job.result()
        table = client.get_table(destination_table)
        logging.info(f"Carga completada. Filas en destino: {table.num_rows}")
    except Exception:
        logging.exception("Error durante la carga a BigQuery")
        raise