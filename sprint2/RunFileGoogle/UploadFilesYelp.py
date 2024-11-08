import os
from google.cloud import storage, bigquery
import pandas as pd

# Configuración de clientes de GCP
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Variables de configuración
bucket_name = "mi-bucket-data-crudos"
bucket_name_clean = "mi-bucket-data-clean"
file_path_1 = "yelp/business.pkl"  # Primer archivo en Cloud Storage
file_path_2 = "yelp/df_reviews.parquet"   # Segundo archivo en Cloud Storage
clean_business_path = 'yelp/business/business_limpio.json'
clean_reviews_path = 'yelp/review/reviews_limpio.json'
dataset_id = 'base_datos_yelp'
table_id_business = 'business'
table_id_reviews = 'reviews'

# Rutas locales temporales
local_path_1 = f'temp/{file_path_1}'
local_path_2 = f'temp/{file_path_2}'
local_clean_business_path = f'temp/{clean_business_path}'
local_clean_reviews_path = f'temp/{clean_reviews_path}'

def descargar_archivo(bucket_name, blob_name, destination_file_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
    blob.download_to_filename(destination_file_name)
    print(f"Archivo descargado: {bucket_name}/{blob_name} a {destination_file_name}")

def limpiar_datos_business(df):
    duplicated_columns = df.columns[df.columns.duplicated()]
    df = df.loc[:, ~df.columns.duplicated()]
    print(f"Columnas duplicadas eliminadas en business: {duplicated_columns}")

    df_business = df[df["state"].isin(["FL"])].copy()
    df_business = df_business[df_business["categories"].str.contains(r"(?i)\brestaurants\b", na=False)].copy()
    df_business["address"] = df_business["address"].fillna('Desconocido')
    df_business["postal_code"] = df_business["postal_code"].fillna('Desconocido')

    return df_business

def limpiar_datos_reviews(df):
    duplicated_columns = df.columns[df.columns.duplicated()]
    df = df.loc[:, ~df.columns.duplicated()]
    print(f"Columnas duplicadas eliminadas en reviews: {duplicated_columns}")

    # Puedes aplicar aquí otros filtros o transformaciones para `reviews` si es necesario.
    return df

def cargar_a_cloud_storage(bucket_name_clean, source_file_path, destination_blob_name):
    bucket = storage_client.bucket(bucket_name_clean)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"Archivo subido: {source_file_path} a {bucket_name_clean}/{destination_blob_name}")

def cargar_a_bigquery(dataset_id, table_id, file_path):
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True
    )
    
    with open(file_path, "rb") as source_file:
        load_job = bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)
    
    load_job.result()
    print(f"Datos cargados en BigQuery: {dataset_id}.{table_id}")

def procesar_archivos():
    descargar_archivo(bucket_name, file_path_1, local_path_1)
    descargar_archivo(bucket_name, file_path_2, local_path_2)

    df1 = pd.read_pickle(local_path_1)
    df2 = pd.read_parquet(local_path_2)

    df1 = df1.drop_duplicates(subset='business_id')
    df2 = df2.drop_duplicates(subset='business_id')

    df_business = limpiar_datos_business(df1)
    df_reviews = limpiar_datos_reviews(df2)

    os.makedirs(os.path.dirname(local_clean_business_path), exist_ok=True)
    df_business.to_json(local_clean_business_path, orient='records', lines=True)
    print("Archivo business limpio guardado localmente.")

    os.makedirs(os.path.dirname(local_clean_reviews_path), exist_ok=True)
    df_reviews.to_json(local_clean_reviews_path, orient='records', lines=True)
    print("Archivo reviews limpio guardado localmente.")

    cargar_a_cloud_storage(bucket_name_clean, local_clean_business_path, clean_business_path)
    cargar_a_cloud_storage(bucket_name_clean, local_clean_reviews_path, clean_reviews_path)

    cargar_a_bigquery(dataset_id, table_id_business, local_clean_business_path)
    cargar_a_bigquery(dataset_id, table_id_reviews, local_clean_reviews_path)
    print("Proceso completado.")

def main(request):
    try:
        procesar_archivos()
        return "Proceso completado exitosamente.", 200
    except Exception as e:
        print(f"Error: {e}")
        return f"Error al procesar el archivo: {e}", 500

if __name__ == "__main__":
    procesar_archivos()



