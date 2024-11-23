# Importar librerías necesarias
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from wordcloud import WordCloud
from matplotlib import rcParams
import plotly.express as px
import plotly.graph_objects as go
from streamlit_folium import st_folium
import folium
import os

# Configurar la ruta del archivo de credenciales
key_path = "C:/Users/javie/Desktop/Proyecto_Grupal/proyecto-resenas-440817-4206c7cd9f78.json"
credentials = service_account.Credentials.from_service_account_file(key_path)

# Configuración inicial de Streamlit
st.title("Análisis y KPIs")
st.sidebar.title("Navegación")
menu = st.sidebar.radio(
    "Selecciona una sección", [
        "Resumen de Datos",
        "Ranking de Restaurantes",
        "Interacciones por Reseñas",
        "Comparación entre Yelp y Google Reviews",
        "Nubes de Palabras",
        "Mapa de Calificaciones y Reseñas",
        "Tendencias de Reseñas y Calificaciones",
        "Correlación de Variables",
        "KPIs Consolidado"
    ]
)

# Conexión a BigQuery (se usará la variable de entorno automáticamente)
try:
    client = bigquery.Client()
    st.success("Conexión con Google BigQuery configurada correctamente.")
except Exception as e:
    st.error(f"Error al configurar credenciales de BigQuery: {e}")
    st.stop()

# IDs de proyecto y datasets
project_id = 'proyecto-resenas-440817'
dataset_id_yelp = 'base_datos_yelp'
dataset_id_resenas = 'proyecto_resenas'

# Cargar datos con caching para mejorar la velocidad
@st.cache_data
def load_data():
    """
    Carga los datos desde BigQuery usando consultas específicas para cada tabla.
    """
    queries = {
        "business": f"SELECT * FROM `{project_id}.{dataset_id_yelp}.business`",
        "reviews": f"SELECT * FROM `{project_id}.{dataset_id_yelp}.reviews`",
        "google_negocios": f"SELECT * FROM `{project_id}.{dataset_id_resenas}.google_negocios`",
        "google_reviews": f"SELECT * FROM `{project_id}.{dataset_id_resenas}.google_reviews`"
    }
    # Ejecutar las consultas y convertir los resultados en DataFrames
    data = {name: client.query(query).to_dataframe() for name, query in queries.items()}
    return data

# Cargar los datos
data = load_data()
df_business = data["business"]
df_reviews = data["reviews"]
df_google_negocios = data["google_negocios"]
df_google_reviews = data["google_reviews"]

# Configurar fuente para gráficos
rcParams['font.family'] = 'Arial'

# Sección: Resumen de Datos
if menu == "Resumen de Datos":
    st.subheader("Vista Previa de Datos")
    
    st.write("**Datos de Negocios (Yelp):**")
    st.dataframe(df_business[['name', 'categories', 'state', 'review_count', 'stars']].head(10))
    
    st.write("**Datos de Reseñas (Yelp):**")
    st.dataframe(df_reviews[['review_id', 'business_id', 'stars', 'text', 'date']].head(10))
    
    st.write("**Datos de Negocios (Google):**")
    st.dataframe(df_google_negocios[['name', 'category', 'avg_rating', 'num_of_reviews']].head(10))

# Sección: Ranking de Restaurantes
elif menu == "Ranking de Restaurantes":
    st.subheader("Ranking de Restaurantes por Calificación y Reseñas")
    df_performance = df_business[['name', 'review_count', 'stars']].copy()
    df_performance['performance_score'] = df_performance['review_count'] * df_performance['stars']
    ranking = df_performance.sort_values(by='performance_score', ascending=False).head(10)
    st.table(ranking)

    # Gráfico de barras del ranking
    fig = px.bar(ranking, x='name', y='performance_score', title="Ranking de Restaurantes por Desempeño",
                 labels={'performance_score': 'Puntaje de Desempeño', 'name': 'Restaurante'}, color='performance_score')
    st.plotly_chart(fig)

# Sección: Interacciones por Reseñas
elif menu == "Interacciones por Reseñas":
    st.subheader("Análisis de Interacciones en Reseñas")
    df_reviews['total_interactions'] = df_reviews[['useful', 'funny', 'cool']].sum(axis=1)
    interactions = df_reviews.groupby('business_id')['total_interactions'].sum().reset_index()
    interactions = interactions.merge(df_business[['business_id', 'name']], on='business_id', how='left')
    top_interactions = interactions.sort_values(by='total_interactions', ascending=False).head(10)
    st.table(top_interactions[['name', 'total_interactions']])

    # Gráfico de barras de interacciones
    fig = px.bar(top_interactions, x='name', y='total_interactions', title="Top 10 Restaurantes por Interacciones",
                 labels={'total_interactions': 'Interacciones Totales', 'name': 'Restaurante'}, color='total_interactions')
    st.plotly_chart(fig)

# Sección: Comparación entre Yelp y Google Reviews
elif menu == "Comparación entre Yelp y Google Reviews":
    st.subheader("Comparación de Calificaciones y Reseñas entre Yelp y Google")
    df_comparison = df_business[['business_id', 'name', 'stars']].merge(df_google_negocios[['name', 'avg_rating', 'num_of_reviews']], on='name', how='left')
    df_comparison['rating_difference'] = df_comparison['stars'] - df_comparison['avg_rating']
    st.dataframe(df_comparison[['name', 'stars', 'avg_rating', 'rating_difference']])

    # Gráfico de barras de diferencias de calificación
    fig = px.bar(df_comparison, x='name', y='rating_difference', title="Diferencia de Calificaciones entre Yelp y Google",
                 labels={'rating_difference': 'Diferencia de Calificación', 'name': 'Restaurante'}, color='rating_difference')
    st.plotly_chart(fig)

# Sección de Nubes de Palabras (para visualizar las nubes de palabras de reseñas)
elif menu == "Nubes de Palabras":
    st.subheader("Nubes de Palabras")
    all_reviews = " ".join(df_reviews['text'].dropna())
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_reviews)
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot()

# Sección: Mapa de Calificaciones y Reseñas
elif menu == "Mapa de Calificaciones y Reseñas":
    st.subheader("Mapa Interactivo de Calificaciones y Reseñas")
    
    # Filtrar negocios con coordenadas válidas
    if 'latitude' in df_business.columns and 'longitude' in df_business.columns:
        df_map = df_business[['name', 'latitude', 'longitude', 'stars', 'review_count']].dropna()
        
        # Crear un mapa base
        map_center = [df_map['latitude'].mean(), df_map['longitude'].mean()]
        m = folium.Map(location=map_center, zoom_start=10)
        
        # Añadir marcadores al mapa
        for _, row in df_map.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=f"""
                <b>{row['name']}</b><br>
                Calificación: {row['stars']}<br>
                Reseñas: {row['review_count']}
                """,
                icon=folium.Icon(color='blue', icon='info-sign')
            ).add_to(m)
        
        # Mostrar el mapa en Streamlit
        st_folium(m, width=700, height=500)
    else:
        st.error("No se encontraron columnas de latitud o longitud en los datos.")

# Sección: Tendencias de Reseñas y Calificaciones
elif menu == "Tendencias de Reseñas y Calificaciones":
    st.subheader("Reseñas y Calificaciones")
    df_reviews['date'] = pd.to_datetime(df_reviews['date']).dt.tz_localize(None)
    df_reviews['month_year'] = df_reviews['date'].dt.to_period('M').astype(str)
    trend = df_reviews.groupby('month_year')['review_id'].count().reset_index()
    fig = px.line(trend, x='month_year', y='review_id', title="Tendencia de Reseñas por Mes", markers=True)
    st.plotly_chart(fig)

# Sección de Correlación de Variables
elif menu == "Correlación de Variables":
    st.subheader("Correlación entre Variables")
    # Calcular la correlación entre calificación y cantidad de reseñas
    df_corr = df_business[['stars', 'review_count']].dropna()
    correlation = df_corr.corr()
    st.write("Correlación entre Calificación y Reseñas:")
    st.write(correlation)

    # Gráfico de dispersión
    fig = px.scatter(df_corr, x='review_count', y='stars', title="Correlación entre Calificación y Número de Reseñas",
                     labels={'review_count': 'Número de Reseñas', 'stars': 'Calificación'})
    st.plotly_chart(fig)

# Sección de KPIs Consolidados
elif menu == "KPIs Consolidado":
    st.subheader("KPIs Consolidados")

    # KPIs para Yelp
    # Calcular la calificación promedio y el número total de reseñas
    avg_rating_yelp = df_business['stars'].mean()
    total_reviews_yelp = df_business['review_count'].sum()

    # KPIs para Google
    avg_rating_google = df_google_negocios['avg_rating'].mean()
    total_reviews_google = df_google_negocios['num_of_reviews'].sum()

    # KPIs generales
    total_businesses_yelp = df_business.shape[0]
    total_businesses_google = df_google_negocios.shape[0]

    # Mostrar los KPIs
    st.write("**KPIs de Yelp:**")
    st.write(f"Calificación promedio en Yelp: {avg_rating_yelp:.2f}")
    st.write(f"Número total de reseñas en Yelp: {total_reviews_yelp}")
    st.write(f"Número de negocios en Yelp: {total_businesses_yelp}")

    st.write("**KPIs de Google:**")
    st.write(f"Calificación promedio en Google: {avg_rating_google:.2f}")
    st.write(f"Número total de reseñas en Google: {total_reviews_google}")
    st.write(f"Número de negocios en Google: {total_businesses_google}")

    # Gráfico de barras para comparar las calificaciones promedio
    kpi_data = {
        "Plataforma": ["Yelp", "Google"],
        "Calificación Promedio": [avg_rating_yelp, avg_rating_google],
        "Número de Reseñas": [total_reviews_yelp, total_reviews_google]
    }
    kpi_df = pd.DataFrame(kpi_data)

    # Calificación Promedio
    fig1 = px.bar(kpi_df, x="Plataforma", y="Calificación Promedio", title="Comparación de Calificación Promedio",
                  labels={"Calificación Promedio": "Calificación Promedio", "Plataforma": "Plataforma"})
    st.plotly_chart(fig1)

    # Número de Reseñas
    fig2 = px.bar(kpi_df, x="Plataforma", y="Número de Reseñas", title="Comparación de Número de Reseñas",
                  labels={"Número de Reseñas": "Número de Reseñas", "Plataforma": "Plataforma"})
    st.plotly_chart(fig2)

    # KPIs adicionales: Comparación de negocios en ambas plataformas
    fig3 = go.Figure(data=[
        go.Bar(name="Yelp", x=["Yelp"], y=[total_businesses_yelp], marker=dict(color='rgb(34, 147, 255)')),
        go.Bar(name="Google", x=["Google"], y=[total_businesses_google], marker=dict(color='rgb(231, 76, 60)'))
    ])
    fig3.update_layout(barmode='group', title="Comparación de Negocios entre Yelp y Google", xaxis_title="Plataforma", yaxis_title="Número de Negocios")
    st.plotly_chart(fig3)

