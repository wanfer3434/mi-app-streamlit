## <h1 align="center"><b><i>Sprint 2</i></b></h1> 

## Data Lake

### :eight_spoked_asterisk: **_Almacenamiento data cruda_**
Se crea un bucket en Google Cloud Platform (GCP) para almacenar datos crudos de reseñas y otros datos relacionados de Google y Yelp. Este es el primer paso de un pipeline de procesamiento de datos para analizar y extraer información valiosa sobre la experiencia del cliente en restaurantes en Florida.

**_mi-bucket-data-cruda:_** 
#### **Google:**
- **metadata-sitios**: La carpeta tiene 11 archivos .json donde se dispone la metadata contiene información del comercio, incluyendo localización, atributos y categorías.
- **review-estadosos**: Los archivos donde se disponibiliza las reviews de los usuarios (51 carpetas, 1 por cada estado de USA, con varios archivos .json cada uno).

#### **Yelp:**
- **business**: Contiene información del comercio, incluyendo localización, atributos y categorías.
- **review**: Contiene las reseñas completas, incluyendo el user_id que escribió el review y el business_id por el cual se escribe la reseña.
- **checkin**: Registros en el negocio.
- **user**: Data del usuario incluyendo referencias a otros usuarios amigos y a toda la metadata asociada al usuario.
- **tips**: Tips (consejos) escritos por el usuario. Los tips son más cortas que las reseñas y tienden a dar sugerencias rápidas.

### :eight_spoked_asterisk: **_Almacenamiento data limpia_**

Para manejar los datos procesados, creamos un segundo bucket en Google Cloud Storage llamado `mi-bucket-data-clean`. Este bucket servirá como un Data Lake donde almacenaremos las versiones limpias y transformadas de los datos de reseñas. Estos datos limpios están preparados para análisis adicionales y son más fáciles de gestionar en comparación con los datos crudos.

Los datos crudos en el bucket de `mi-bucket-data-crudos` se procesan para eliminar duplicados y valores nulos, y filtrar solo las reseñas y atributos relevantes para nuestro análisis. Nos enfocamos en dos tablas: `business` y `reviews`, tanto de Yelp como de Google, en donde encontraremos:

- **mi-bucket-data-clean:** Contiene la data ya procesada y normalizada. Este bucket se divide en dos secciones:
  - **Google**: Incluye dos tablas:
     - **Metadata-sitios:** Donde se guarda toda la información sobre los negocios.
     - **florida**: Se guardo la informacion de las reviews de florida.
   
  - **Yelp**: Incluye dos tablas:
     - **business:** Donde se guarda toda la información sobre los negocios.
     - **review**: Se guardo la informacion de las reviews de florida.
   
## :eight_spoked_asterisk: **_Data Warehouse_**

Una vez que hemos almacenado y procesado los datos en nuestro Data Lake `mi-bucket-data-clean`, el siguiente paso es cargarlos en **BigQuery**. **BigQuery** es el servicio de Data Warehouse de Google Cloud, ideal para análisis y consultas a gran escala. Aquí, los datos están listos para ser utilizados en análisis avanzados, reportes y dashboards de visualización.

Al transferir nuestros datos limpios desde el Data Lake a BigQuery, logramos varias cosas:

- **Centralización del Almacenamiento:** BigQuery permite gestionar grandes volúmenes de datos en un entorno centralizado, seguro y optimizado para consultas SQL.
- **Preparación para Análisis:** Con los datos almacenados en un formato estructurado, ahora podemos aplicar modelos de machine learning, realizar análisis de tendencias y obtener insights sobre las reseñas de los restaurantes de Google y Yelp.


## :eight_spoked_asterisk: **_Carga autoincremental_**

