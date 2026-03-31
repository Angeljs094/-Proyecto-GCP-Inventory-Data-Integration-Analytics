# config.py

# --- Configuración de Conexiones y Rutas ---
GCS_CONN_ID = 'google_cloud_default'
GCS_BUCKET = 'composer-atjs'
GCS_OBJECT = 'data.json'

POSTGRES_CONN_ID = 'connpost_an'
POSTGRES_TABLE = 'productos'

# --- Configuración BigQuery ---
BQ_CONN_ID = 'google_cloud_default'
BQ_PROJECT = 'proyectodmc-488102'    # Tu proyecto GCP
BQ_DATASET = 'productos_dw'          # Tu dataset
BQ_TABLE = 'productos'               # Tabla destino
BQ_WRITE_DISPOSITION = 'WRITE_TRUNCATE'