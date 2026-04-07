import google.auth
from google.cloud import bigquery
from google.cloud import storage
import io
import pandas as pd
import json
import os
from google.generativeai import GenerativeModel
from google.api_core.exceptions import NotFound

# Configuration
PROJECT_ID = "shashirr01"
BUCKET_NAME = "shahsi148"
FINANCE_DATASET_ID = "sharechat_finance_data"
METADATA_FILE = "agents/sharechat_finance_agent/schema_metadata.json"

credentials, _ = google.auth.default()

def generate_and_update_metadata(file_name: str):
    """Generates column descriptions and updates the local registry."""
    print(f"Generating metadata for gs://{BUCKET_NAME}/{file_name}...")
    storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    
    try:
        content = blob.download_as_bytes(start=0, end=20480).decode('utf-8', errors='ignore')
        df = pd.read_csv(io.StringIO(content), nrows=10)
    except Exception as e:
        print(f"Error reading file from GCS: {e}")
        return None
    
    prompt = (
        f"Analyze these finance CSV columns and sample data:\n\n"
        f"Columns: {df.columns.tolist()}\n"
        f"Sample:\n{df.head(3).to_string()}\n\n"
        "Return a JSON object of {column_name: description}. "
        "Descriptions should be concise and help a SQL agent understand the data's financial meaning."
    )
    
    model = GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)
    
    try:
        clean_json = response.text.replace('```json', '').replace('```', '').strip()
        new_meta = json.loads(clean_json)
        
        existing_meta = {}
        if os.path.exists(METADATA_FILE):
            with open(METADATA_FILE, 'r') as f:
                existing_meta = json.load(f)
        
        existing_meta.update(new_meta)
        
        os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
        with open(METADATA_FILE, 'w') as f:
            json.dump(existing_meta, f, indent=2)
        
        print(f"Successfully updated metadata with {len(new_meta)} fields.")
        return existing_meta
    except Exception as e:
        print(f"Error generating metadata: {e}")
        return None

def load_to_bq(file_name: str, table_id: str):
    """Loads CSV from GCS to BQ and applies metadata."""
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    dataset_ref = client.dataset(FINANCE_DATASET_ID)
    table_ref = dataset_ref.table(table_id)
    
    # Ensure Dataset
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset: {FINANCE_DATASET_ID}")

    # Load Table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        skip_leading_rows=1,
    )
    uri = f"gs://{BUCKET_NAME}/{file_name}"
    print(f"Loading {uri} into {PROJECT_ID}.{FINANCE_DATASET_ID}.{table_id}...")
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    
    # Apply Schema Metadata
    table = client.get_table(table_ref)
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            meta = json.load(f)
        new_schema = []
        for field in table.schema:
            new_schema.append(bigquery.SchemaField(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                description=meta.get(field.name, ""),
                fields=field.fields
            ))
        table.schema = new_schema
        client.update_table(table, ["schema"])
        print("Enriched BigQuery schema with semantic metadata.")
    
    print("Ingestion complete.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python3 bq_data_loader.py <file_name> <table_id>")
    else:
        file = sys.argv[1]
        table = sys.argv[2]
        generate_and_update_metadata(file)
        load_to_bq(file, table)
