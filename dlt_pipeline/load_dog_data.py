import dlt
import requests

def dog_breeds_source():
    response = requests.get("https://api.thedogapi.com/v1/breeds")
    response.raise_for_status()
    return response.json()

pipeline = dlt.pipeline(
    pipeline_name="dog_breeds",
    destination="bigquery",
    dataset_name="bronze"
)

data = dog_breeds_source()
load_info = pipeline.run(data, table_name="dog_api_raw")

print("Loaded rows:", load_info["loaded_rows"])
