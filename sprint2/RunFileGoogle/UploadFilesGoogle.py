import os
import pandas as pd
import geopandas as gpd
from google.cloud import storage
import pyarrow as pa
from io import BytesIO
import pyarrow.parquet as pq

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "proyecto-resenas-440817-8c1463da89dd.json"

storage_client = storage.Client()

# Nombre de los buckets
bucket_raw = "mi-bucket-data-crudos"
bucket_clean = "mi-bucket-data-clean"

# Función para filtrar solo los locales en Florida
def florida_filter(data):
    us_states = gpd.read_file("us_states.geojson")
    florida = us_states[us_states["NAME"] == "Florida"]
    dt1_gdf = gpd.GeoDataFrame(
        data, 
        geometry=gpd.points_from_xy(data["longitude"], data["latitude"]),
        crs="EPSG:4326"
    )
    fl_data = gpd.sjoin(dt1_gdf, florida, predicate="within")
    fl_data.drop(columns=["STATEFP", "index_right", "geometry"], inplace=True)
    fl_data.rename(columns={"NAME": "state name"}, inplace=True)
    return pd.DataFrame(fl_data)


#Funcion para filtrar los restaurantes
def filtro_restaurante(data):
    keywords = ['restaurant', 'coffee','cafe', 'ice cream', 'wine bars',
              'sushi bars', 'tea', 'bakery', 'food', 'diner', 'tortilla',
              'vegetarian', 'soup', 'salad', 'cake', 'donut', 
              'sandwiches', 'pizza', 'burger', 'hot dog', 
              'breakfast', 'brunch', 'restaurants', 'barbeque',"bistro","grill","cocktail","taco"]
    
    # Convertir keywords a minúsculas para evitar errores por mayúsculas/minúsculas
    keywords = [kw.lower() for kw in keywords]

    # Filtrar los datos mediante las keywords
    category_restaurants = data[data["category"].apply(
        lambda categories: any(keyword in item.lower() for item in categories for keyword in keywords) 
        if isinstance(categories, list) else False
    )]
    
    return category_restaurants


# Función para procesar archivos JSON desde el bucket crudo y subir al bucket limpio
def process_and_upload_files(bucket_raw, bucket_clean, folder_path):
    raw_bucket = storage_client.bucket(bucket_raw)
    clean_bucket = storage_client.bucket(bucket_clean)
    
    # Listar los archivos JSON en el bucket crudo
    blobs = raw_bucket.list_blobs(prefix=folder_path)
    
    for blob in blobs:
        if blob.name.endswith(".json"):
            # Construir el nombre del archivo para el bucket limpio, con `google/metadata-sitios`
            clean_path = f"{folder_path}/{blob.name.split('/')[-1].replace('.json', '.parquet')}"
            clean_blob = clean_bucket.blob(clean_path)
            
            # Verificar si el archivo ya existe en el bucket limpio
            if clean_blob.exists():
                print(f"Archivo ya existe y se omite: {clean_path}")
                continue
            
            # Descargar el archivo JSON
            data = pd.read_json(BytesIO(blob.download_as_bytes()), lines=True)
            
            # Eliminar duplicados y aplicar el filtro de Florida
            data.drop_duplicates(subset=["gmap_id"], inplace=True)
            data_filtered = florida_filter(data)
            
            # Aplicar el filtro de restaurantes
            data_filtered = filtro_restaurante(data_filtered)
            
            # Convertir a formato parquet para almacenamiento eficiente
            buffer = BytesIO()
            table = pa.Table.from_pandas(data_filtered)
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            # Subir el archivo parquet al bucket de datos limpios
            clean_blob.upload_from_file(buffer, content_type="application/octet-stream")
            print(f"Archivo procesado y subido: {clean_path}")

# Ejecutar la función de procesamiento y subida
process_and_upload_files(bucket_raw, bucket_clean, "google/metadata-sitios")

# Nombre de los buckets y carpetas
bucket_raw = "mi-bucket-data-crudos"
bucket_clean = "mi-bucket-data-clean"
florida_folder = "google/reviews-estados"
metadata_sitios_folder = "google/metadata-sitios"

# Función para filtrar reseñas mediante `gmap_id`
def reseñasfiltradas(data, filter_data):
    return data[data['gmap_id'].isin(filter_data['gmap_id'])]

# Función para filtrar archivos de Florida y subirlos directamente a Parquet
def filter_and_upload_florida(bucket_raw, bucket_clean, florida_folder, metadata_sitios_folder):
    raw_bucket = storage_client.bucket(bucket_raw)
    clean_bucket = storage_client.bucket(bucket_clean)
    
    # Listar los archivos JSON en el bucket crudo que contienen "florida"
    blobs = raw_bucket.list_blobs(prefix=florida_folder)
    
    # Obtener los `gmap_id` de los archivos de `metadata-sitios` ya cargados en el bucket limpio
    metadata_blobs = clean_bucket.list_blobs(prefix=metadata_sitios_folder)
    metadata_gmap_ids = pd.DataFrame()
    
    for blob in metadata_blobs:
        if blob.name.endswith(".parquet"):
            # Leer los gmap_id de cada archivo Parquet
            data = pd.read_parquet(BytesIO(blob.download_as_bytes()))
            metadata_gmap_ids = pd.concat([metadata_gmap_ids, data[['gmap_id']]], ignore_index=True)
    
    # Filtrar duplicados de `gmap_id`
    metadata_gmap_ids.drop_duplicates(inplace=True)
    
    # Filtrar y subir archivos de "Florida" basados en `gmap_id` de metadata
    for blob in blobs:
        if "florida" in blob.name.lower() and blob.name.endswith(".json"):
            # Crear el nombre de archivo limpio sin `reviews-estados`
            clean_path = f"google/florida/{blob.name.split('/')[-1].replace('.json', '.parquet')}"
            clean_blob = clean_bucket.blob(clean_path)
            
            if clean_blob.exists():
                print(f"Archivo ya existe y se omite: {clean_path}")
                continue
            
            # Descargar y filtrar el JSON
            data = pd.read_json(BytesIO(blob.download_as_bytes()), lines=True)
            filtered_data = reseñasfiltradas(data, metadata_gmap_ids)
            
            # Convertir a Parquet y subir
            buffer = BytesIO()
            table = pa.Table.from_pandas(filtered_data)
            pq.write_table(table, buffer)
            buffer.seek(0)
            clean_blob.upload_from_file(buffer, content_type="application/octet-stream")
            print(f"Archivo filtrado y subido: {clean_path}")

filter_and_upload_florida(bucket_raw, bucket_clean, florida_folder, metadata_sitios_folder)