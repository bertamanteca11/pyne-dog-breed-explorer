import dlt
import requests
import json
import os
from google.cloud import storage
import datetime

# Cargar credenciales desde el archivo JSON
with open("gcp_credentials.json") as f:
    creds = json.load(f)

# Configurar variables de entorno para dlt
os.environ["CREDENTIALS__PROJECT_ID"] = creds["project_id"]
os.environ["CREDENTIALS__PRIVATE_KEY"] = creds["private_key"]
os.environ["CREDENTIALS__CLIENT_EMAIL"] = creds["client_email"]

# Obtener datos del API
def dog_breeds_source():
    response = requests.get("https://api.thedogapi.com/v1/breeds")
    response.raise_for_status()
    return response.json()


def save_to_gcs(data):
    client = storage.Client()
    bucket = client.bucket("dog-api-storage")  # ← usa el nombre de tu bucket
    run_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    blob = bucket.blob(f"dog_api_raw/{run_date}/data.json")
    blob.upload_from_string(json.dumps(data), content_type="application/json")

save_to_gcs(data)

# Crear pipeline
pipeline = dlt.pipeline(
    pipeline_name="dog_breeds",
    destination="bigquery",
    dataset_name="bronze"
)

data = dog_breeds_source()
load_info = pipeline.run(data, table_name="dog_api_raw")

print("Load info:", load_info)
