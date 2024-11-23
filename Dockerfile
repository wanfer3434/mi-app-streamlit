# Imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar los archivos de la aplicación al contenedor
COPY . /app

# Copiar el archivo de credenciales JSON al contenedor
COPY proyecto-resenas-440817-4206c7cd9f78.json /app/proyecto-resenas-440817-4206c7cd9f78.json

# Configurar la variable de entorno para las credenciales de Google Cloud
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/proyecto-resenas-440817-4206c7cd9f78.json"

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto que Streamlit usa por defecto
EXPOSE 8501

# Comando para ejecutar la aplicación
CMD ["streamlit", "run", "app_streamlit.py", "--server.port=8501", "--server.address=0.0.0.0"]
