from fastapi import FastAPI, HTTPException, Body
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import storage
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import fastavro
import io
from google.api_core.exceptions import BadRequest

load_dotenv()
app = FastAPI()

# Variables de enttorno
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
GCP_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BACKUP_FOLDER = "Backup"

#GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS
BQ_CLIENT = bigquery.Client()
STORAGE_CLIENT = storage.Client()

# Logs de inserts fallidos
LOG_FILE = "invalid_transactions.log"
logging.basicConfig(filename=LOG_FILE, level=logging.WARNING, format="%(asctime)s - %(message)s")

# Esquema de las tablas (si cambia en bigquery CAMBIAR ACA TAMBIEN!)
SCHEMAS = {
    "departments": [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("department", "STRING", mode="REQUIRED"),
    ],
    "jobs": [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("job", "STRING", mode="REQUIRED"),
    ],
    "hired_employees": [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("datetime", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("department_id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("job_id", "INTEGER", mode="NULLABLE"),
    ]
}

#Mapear tipos de datos de bigquery a python
TYPE_MAPPING = {
        "INTEGER": int,
        "STRING": str,
    }

# Limite de registros a insertar
MAX_RECORDS_PER_INSERT = 1000  

# Validar esquema de las tablas y tipos dedatos 
def validate_data(table: str, data: list) -> tuple:
    schema_fields = SCHEMAS.get(table)
    if not schema_fields:
        return False, "La tabla no tiene esquema previamente definido"
    
    required_columns = {field.name: TYPE_MAPPING[field.field_type] for field in schema_fields}
    if not required_columns:
        return False, "Columnas sin mapeo"
    
    for row in data:
        for column, col_type in required_columns.items():
            # Verificar que las columnas existan y sean del tipo que es
            if column not in row or row[column] is None or type(row[column]) != col_type:
                log_invalid_transaction(row, f"Tipo incorrecto o campo faltante: {column}")
                return False, f"Registro inválido, erroren '{column}' en: {row}"
            
            #date format en hired_employees 
            if table == "hired_employees" and column == "datetime":
                try:
                    datetime.strptime(row[column], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    log_invalid_transaction(row, " Formato de fecha incorrecto en 'datetime'")
                    return False, f"Formato incorrecto en 'datetime', usar YYYY-MM-DD HH:MM:SS en: {row}"

    return True, "Validacion exitosa"

# Verificar si hay ids duplicados
def check_duplicates(table: str, data: list) -> list:
    table_id = f"{DATASET_ID}.{table}"
    ids_to_check = [row["id"] for row in data]

    if not ids_to_check:
        return []

    query = f"""
    SELECT id FROM `{table_id}`
    WHERE id IN UNNEST(@id_list)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("id_list", "INT64", ids_to_check)
        ]
    )


    query_job = BQ_CLIENT.query(query, job_config=job_config)
    existing_ids = [row["id"] for row in query_job]
    return existing_ids

def log_invalid_transaction(data, reason):
    logging.warning(f"Registro invalido: {data} - Motivo: {reason}")

def table_exists(table_name: str):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    try:
        BQ_CLIENT.get_table(table_id)
    except NotFound:
        # Crear la tabla con el esquema definido
        table = bigquery.Table(table_id, schema=SCHEMAS[table_name])
        BQ_CLIENT.create_table(table)
        print(f"Tabla '{table_name}' no existia, se creo en bigquery")

#Cargar datos historicos de un archivo en especifico (la tabla debe tener el mismo nombre del arhcivo)
@app.post("/load/{table_name}")
async def load_csv_to_bigquery(table_name: str):
    if table_name not in SCHEMAS:
        raise HTTPException(status_code=400, detail=f"Esquema no definido para la tabla '{table_name}'")

    table_exists(table_name)

    file_name = f"{table_name}.csv"
    uri = f"gs://{BUCKET_NAME}/{file_name}"
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=SCHEMAS[table_name],
        source_format=bigquery.SourceFormat.CSV,
        write_disposition="WRITE_TRUNCATE" 
    )

    try:
        load_job = BQ_CLIENT.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result() 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error cargando '{file_name}': {str(e)}")

    return {"message": f"Datos de '{file_name}' cargados correctamente en '{table_name}'"}

#Cargar datos historicos de todos los archivos
@app.post("/load_all")
async def load_all_csvs():
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs())

    csv_files = [blob.name for blob in blobs if blob.name.endswith(".csv")]

    if not csv_files:
        raise HTTPException(status_code=404, detail="No se encontraron archivos csv en el bucket")

    results = []
    for file_name in csv_files:
        table_name = file_name.replace(".csv", "")
        if table_name in SCHEMAS:
            response = await load_csv_to_bigquery(table_name)
            results.append(response)
        else:
            results.append({"error": f"No hay un esquema creado para \"{file_name}\""})

    return {"message": "Carga completa", "results": results}

# Insertar datos en tabla
@app.post("/insert/{table}")
async def insert_data(table: str, data: list = Body(...)):
    if len(data) > MAX_RECORDS_PER_INSERT:
        raise HTTPException(status_code=400, detail=f"Maximo {MAX_RECORDS_PER_INSERT} registros permitidos por solicitud")

    valid, message = validate_data(table, data)
    if not valid:
        raise HTTPException(status_code=400, detail=message)

    duplicate_ids = check_duplicates(table, data)
    if duplicate_ids:
        for row in data:
            if row["id"] in duplicate_ids:
                log_invalid_transaction(row, "id ya existe en la base de datos")
        raise HTTPException(status_code=400, detail={"error": "ids duplicados", "ids": duplicate_ids})

    table_id = f"{DATASET_ID}.{table}"

    #Validar si la tabla existe en bigquery
    try:
        BQ_CLIENT.get_table(table_id)  #Intenta obtener la tabla
    except NotFound:
        raise HTTPException(status_code=404, detail=f"La tabla '{table}' no existe en bigquery")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al verificar la tabla '{table}': {str(e)}")

    errors = BQ_CLIENT.insert_rows_json(table_id, data)

    if errors:
        raise HTTPException(status_code=500, detail={"message": "Error insertando datos", "details": errors})

    return {"message": "Datos insertados correctamente"}


# Crear backup de tabla en el bucket en formato avro
@app.post("/backup/{table}")
async def backup_table(table: str):
    table_id = f"{DATASET_ID}.{table}"
    try:
        BQ_CLIENT.get_table(table_id)
    except NotFound:
        raise HTTPException(status_code=404, detail=f"La tabla '{table}' no existe")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{table}_backup_{timestamp}.avro"
    destination_uri = f"gs://{BUCKET_NAME}/{BACKUP_FOLDER}/{file_name}"

    extract_job = BQ_CLIENT.extract_table(
        table_id, destination_uri,
        job_config=bigquery.ExtractJobConfig(destination_format="avro")
    )
    extract_job.result()

    return {"message": "Backup generado correctamente", "file": destination_uri}


# Restaurar tabla desde backup creado previamente
@app.post("/restore/{table}")
async def restore_table(table: str):
    #Restaura una tabla desde su backup mas reciente
    bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=f"Backup/{table}_"))

    if not blobs:
        raise HTTPException(status_code=404, detail=f"No se encontraron backups para {table}")

    latest_backup = max(blobs, key=lambda x: x.name)
    avro_data = latest_backup.download_as_bytes()
    avro_file = io.BytesIO(avro_data)

    try:
        avro_reader = fastavro.reader(avro_file)
        rows = [record for record in avro_reader]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo avro: {str(e)}")

    # Obtener las columnas en el orden correcto desde bigquery
    # Cuando hay datos en streaming buffer, al recrear la tabla, se cradaba con las columnas en orden incorrecto, order by desc, lo corrigio
    query_columns = f"""SELECT column_name FROM `{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table}' ORDER BY ordinal_position DESC """

    query_job = BQ_CLIENT.query(query_columns)
    columns = [row["column_name"] for row in query_job.result()]

    # Reordenar los datos antes de insertarlos
    ordered_rows = [{col: row[col] for col in columns} for row in rows]

    table_id = f"{DATASET_ID}.{table}"

    try:
        delete_query = f"DELETE FROM `{table_id}` WHERE TRUE"
        delete_job = BQ_CLIENT.query(delete_query)
        delete_job.result()

        # Esperar 5 segundos antes de continuar con la carga por si hay datos en streaming buffer
        import time
        time.sleep(5)

    except BadRequest as e:
        if "would affect rows in the streaming buffer" in str(e):
            schema_query = f"SELECT column_name, data_type FROM `{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table}'"
            schema_job = BQ_CLIENT.query(schema_query)
            schema = [(row["column_name"], row["data_type"]) for row in schema_job]

            drop_query = f"DROP TABLE `{table_id}`"
            BQ_CLIENT.query(drop_query).result()

            create_query = f"CREATE TABLE `{table_id}` ({', '.join(f'{col} {dtype}' for col, dtype in schema)})"
            BQ_CLIENT.query(create_query).result()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = BQ_CLIENT.load_table_from_json(ordered_rows, table_id, job_config=job_config)
    job.result()

    return {"message": f"Tabla '{table}' restaurada correctamente"}