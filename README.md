# Data Engineering Challenge - Sebastian Lopez

Este proyecto implementa una API REST con **FastAPI** para la gestión de datos en **Google BigQuery**, permitiendo la carga, validación, inserción, respaldo y restauración de datos desde archivos **CSV** almacenados en **Google Cloud Storage (GCS)**.

## Requisitos Previos

Para ejecutar el proyecto, es necesario tener configurados los siguientes elementos:

1. Python 3.8+.
2. Google Cloud SDK configurado y autenticado.
3. Validar acceso a datos en BigQuery: https://shorturl.at/MvsAS
4. Validar acceso a Datos en GCP Storage https://shorturl.at/VTo4t

*Estos dos ultimos son mas que todo para hacer validaciones puentuales en los datos*

## 🔧 Instalación

1. **Clonar el repositorio**
   ```bash
   git clone https://github.com/elsebastiann/Globant-DE-Challenge.git

2. **Instalar dependencias**
   ```bash
   pip install -r requirements.txt
   
3. **Configurar las variables de entorno**
   
   Renombra el archivo variables.txt a .env y actualizalo con los valores reales:

   - credenciales gcp: Strg-key/globant-de-challenge-de39bfecfb10.json
   - bucket: globant-de-challenge
   - dataset: ds_globant_de_challenge
   - poyecto: globant-de-challenge

4. Renombra el archivo *credenciales.txt* a *globant-de-challenge-de39bfecfb10.json*.


## Ejecucion

1. **Iniciar la API**
   ```bash
    uvicorn main:app --reload
   
2. **Acceder a Swagger**
   http://127.0.0.1:8000/docs


3. **Endpoints disponibles**
   
   Cargar Datos
   | Metodo | Endpoint | Descripcion|
   |-|-|-|
   | POST | /load_all | Carga todos los archivos CSV en el bucket de GCP a BigQuery, creando las tablas si no existen |
   | POST | /load/{table_name} | Carga un archivo CSV específico en BigQuery, reemplazando los datos existentes |
   
   Insertar Datos
   | Metodo | Endpoint | Descripcion|
   |-|-|-|
   | POST | /insert/{table} | Inserta registros en la tabla especificada con validaciones de integridad |
   
   
   Respaldar y restaurar Datos
   | Metodo | Endpoint | Descripcion|
   |-|-|-|
   | POST | /backup/{table} | Genera un respaldo en formato AVRO y lo almacena en GCP |
   | POST | /restore/{table} | Restaura una tabla desde el último respaldo disponible |

3. **Notas importantes**

   - Validacion de datos: antes de insertar registros en BigQuery, se verifica la estructura y tipos de datos esperados
   - Prevencion de duplicados: se detectan IDs repetidos antes de insertar nuevos registros
   - Respaldo y recuperacion: los datos pueden respaldarse en AVRO y restaurarse usando la ultima copia de seguridad disponible de cada tabla
   - Configuracion segura: Las credenciales y configuraciones sensibles se explica como ser configuradas para no cargarlas directamente en el proyecto. Esta exepcion se hace por ser un challenge tecnico con intensiones meramente de validar expertiz tecnico para la entrevista
  
   - 
