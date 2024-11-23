import os
import pandas as pd
from google.cloud import storage

# Establece la variable de entorno para las credenciales
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "tidy-bliss-440219-b9-41fbf0f1b1ed.json"

# Inicializa el cliente de Google Cloud Storage
storage_client = storage.Client()

bucket_crudo = 'mi-bucket-datos-crudos'
bucket_limpia = 'mi-bucket-data-limpia'

def extract_data_from_gcs(bucket_name, file_name):
    """Extrae datos de Google Cloud Storage."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_text()

    # Verifica el tipo de archivo y lee los datos apropiadamente
    if file_name.endswith('.json'):
        return pd.read_json(data)  # Usar directamente el string
    elif file_name.endswith('.csv'):
        return pd.read_csv(pd.compat.StringIO(data))
    elif file_name.endswith('.pkl'):
        return pd.read_pickle(pd.compat.StringIO(data))
    elif file_name.endswith('.parquet'):
        return pd.read_parquet(pd.compat.StringIO(data))
    else:
        raise ValueError("Formato de archivo no soportado")

def main():
    # Cambia el nombre del archivo aquí según lo que desees procesar
    data = extract_data_from_gcs(bucket_crudo, 'review.json')  # Asegúrate de que 'review.json' esté en el bucket
    print(data)  # Muestra los datos o continúa con los pasos de transformación y carga

if __name__ == '__main__':
    main()
