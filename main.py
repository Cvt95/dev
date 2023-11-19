from google.cloud import bigquery
from google.cloud import firestore
from google.oauth2 import service_account
import os
import time


def get_bigquery_credentials(service_account_path):
    return service_account.Credentials.from_service_account_file(service_account_path)

def get_bigquery_client(credentials, project_id):
    return bigquery.Client(credentials=credentials, project=project_id)

def get_firestore_client(project_id, credentials):
    return firestore.Client(project=project_id, credentials=credentials)

def get_next_collection_name(base_collection_name, firestore_client):
    while True:
        collection_ref = firestore_client.collection(base_collection_name)
        docs = list(collection_ref.limit(1).stream())
        if len(docs) == 0:
            return base_collection_name
        else:
            base_collection_name = increment_collection_name(base_collection_name)

def increment_collection_name(collection_name):
    parts = collection_name.split('_')
    if len(parts) > 0 and parts[-1].isdigit():
        last_part = int(parts[-1])
        parts[-1] = str(last_part + 1)
    else:
        parts.append('1')
    return '_'.join(parts)

def insert_firestore_batch(data_to_insert, firestore_client, execution_id, collection_name):
    batch = firestore_client.batch()

    for sku_input, data in data_to_insert.items():
        doc_ref = firestore_client.collection(collection_name).document(sku_input)
        batch.set(doc_ref, data)

    batch.commit()
    print(f"Execution {execution_id}: Inserted batch data in Firestore - Collection: {collection_name}")

def save_to_bigquery(inserted_records_all, bq_client, dataset_id, table_id):
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.autodetect = True

    bq_client.load_table_from_json(inserted_records_all, f"{dataset_id}.{table_id}", job_config=job_config)

def main():
    start_time = time.time()
    PATH_SOURCES = './'
    SERVICE_ACCOUNT_PATH_BQ = os.path.join(PATH_SOURCES, 'prd-promart-mkt-bq.json')
    SERVICE_ACCOUNT_PATH_FIRE = os.path.join(PATH_SOURCES, 'firebase-adminsdk.json')

    project_id_bq = 'prd-promart-mkt-bq'
    project_id_firestore = 'qprueba-215714'

    bq_credentials = get_bigquery_credentials(SERVICE_ACCOUNT_PATH_BQ)
    bq_client = get_bigquery_client(bq_credentials, project_id_bq)

    firestore_credentials = get_bigquery_credentials(SERVICE_ACCOUNT_PATH_FIRE)
    firestore_client = get_firestore_client(project_id_firestore, firestore_credentials)

    base_collection_name = 'VENTA_PROYECTO_1692739874874433'
    collection_name = get_next_collection_name(base_collection_name, firestore_client)

    query = """
        select *
        from `prd-promart-mkt-bq.cesar_vasquez_mkt.base_entregar_apiv2`
        where skuInput = '99451'
        order by skuInput
    """
#order by skuInput
#where skuInput in ('102978','102979')
    query_job = bq_client.query(query)

    chunk_size = 450 #450
    data_to_insert = {}
    inserted_records_all = []
    execution_id = 0

    for index, row in enumerate(query_job.result(page_size=chunk_size), start=1):
        sku_input = row.skuInput
        linea = row.codLinea
        sku = row.codSku
        descripcion_sku = row.nomSku

        print(f"Read data from BigQuery - Row {index}: SKU Input: {sku_input}, Linea: {linea}, SKU: {sku}, Descripción SKU: {descripcion_sku}")

        if sku_input not in data_to_insert:
            data_to_insert[sku_input] = {
                'processDate': str(row.processDate),
                'loadDate': str(row.loadDate),
                'codProyecto': row.codProyecto,
                'nombreProyecto': row.nombreProyecto,
                'skuInput': sku_input,
                'items': []
            }

        existing_linea = next((item for item in data_to_insert[sku_input]['items'] if item['linea'] == linea), None)

        if existing_linea:
            existing_linea['skus'].append({'sku': sku, 'descripcion': descripcion_sku})
        else:
            item = {
                'linea': linea,
                'descripcion': row.descripcionLinea,
                'skus': [{'sku': sku, 'descripcion': descripcion_sku}]
            }
            data_to_insert[sku_input]['items'].append(item)

        # Insertar cada 500 registros en Firestore usando batch (se debe poner multiplos en base a la cantidad de registros output)
        if index % 450 == 0:
            insert_firestore_batch(data_to_insert, firestore_client, execution_id, collection_name)
            inserted_records_all.extend([{'sku_input': sku_input, 'data': data} for sku_input, data in data_to_insert.items()])
            data_to_insert = {}
            execution_id += 1

    # Insertar registros restantes
    if data_to_insert:
        insert_firestore_batch(data_to_insert, firestore_client, execution_id, collection_name)
        inserted_records_all.extend([{'sku_input': sku_input, 'data': data} for sku_input, data in data_to_insert.items()])

    print("Todos los datos se han procesado e insertado en Firestore.")

    # Guardar los registros insertados en Firestore en BigQuery
    # Inserción en BigQuery
    dataset_id = 'cesar_vasquez_mkt'
    table_id = 'base_entregar_apiv7'

    save_to_bigquery(inserted_records_all, bq_client, dataset_id, table_id)
    print("Los registros insertados en Firestore se han guardado en BigQuery.")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tiempo de ejecución: {elapsed_time} segundos.")

if __name__ == "__main__":
    main()