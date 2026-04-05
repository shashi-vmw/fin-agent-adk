import google.auth
from google.adk import Agent
from google.adk.tools.bigquery import BigQueryToolset, BigQueryCredentialsConfig
from mcp.server.fastmcp import FastMCP
import os
import json
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

# 1. Configuration & Credentials
# Hardcoded project ID as requested
PROJECT_ID = "shashirr01"
credentials, _ = google.auth.default()

# 2. BigQuery Toolset (ADK Connector)
bq_toolset = BigQueryToolset(
    credentials_config=BigQueryCredentialsConfig(credentials=credentials)
)

# 3. Custom Tools for Ingestion
def search_for_existing_finance_data():
    """
    Scans all datasets in the project for tables that match the finance schema 
    (specifically looking for fields like 'cost_usd', 'usage_type', 'amount').
    Helps avoid duplicate ingestion.
    """
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    datasets = list(client.list_datasets())
    found_tables = []

    # Financial indicators we look for in schemas
    target_fields = {"cost_usd", "usage_type", "amount", "timestamp"}

    for dataset in datasets:
        tables = list(client.list_tables(dataset.reference))
        for table_item in tables:
            try:
                table = client.get_table(table_item.reference)
                field_names = {field.name.lower() for field in table.schema}

                # If the table has 3 or more of our target fields, it's likely a match
                intersection = field_names.intersection(target_fields)
                if len(intersection) >= 3:
                    found_tables.append({
                        "table_id": table.full_table_id,
                        "matched_fields": list(intersection),
                        "row_count": table.num_rows,
                        "dataset": dataset.dataset_id
                    })
            except Exception:
                continue

    return {
        "project": PROJECT_ID,
        "found_potential_finance_tables": found_tables,
        "message": "Use one of these if they match your needs instead of ingesting again." if found_tables else "No similar finance tables found."
    }

def ingest_from_gcs(bucket_name: str, file_name: str, dataset_id: str, table_id: str):
    """
    Ingests a large CSV from GCS to BigQuery.
    Checks if the dataset and table exist. Creates dataset if missing. 
    Loads table only if missing or if requested to refresh.
    """
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    # 1. Check/Create Dataset
    try:
        client.get_dataset(dataset_ref)
        dataset_status = "already existed"
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        try:
            client.create_dataset(dataset)
            dataset_status = "created"
        except Conflict:
            dataset_status = "already existed (race condition)"
    except Exception as e:
        return {"error": f"Failed to check/create dataset: {str(e)}"}

    # 2. Check if Table exists
    try:
        client.get_table(table_ref)
        table_exists = True
    except NotFound:
        table_exists = False

    # 3. Load data if table doesn't exist
    if not table_exists:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,
            skip_leading_rows=1,
        )
        uri = f"gs://{bucket_name}/{file_name}"
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for completion
        table_status = "loaded"
        
        # 4. Enrichment with Metadata (only if we just loaded it)
        table = client.get_table(table_ref)
        meta_path = os.path.join(os.path.dirname(__file__), 'schema_metadata.json')
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            new_schema = []
            for field in table.schema:
                if field.name in meta:
                    new_schema.append(bigquery.SchemaField(
                        name=field.name,
                        field_type=field.field_type,
                        mode=field.mode,
                        description=meta[field.name],
                        fields=field.fields
                    ))
                else:
                    new_schema.append(field)
            table.schema = new_schema
            client.update_table(table, ["schema"])
            metadata_status = "applied"
        else:
            metadata_status = "not found"
    else:
        table_status = "already existed (skipping upload)"
        metadata_status = "skipped"

    return {
        "status": "success", 
        "project": PROJECT_ID,
        "dataset": f"{dataset_id} ({dataset_status})",
        "table": f"{table_id} ({table_status})",
        "metadata": metadata_status,
        "full_path": f"{PROJECT_ID}.{dataset_id}.{table_id}"
    }

def generate_schema_metadata(bucket_name: str, file_name: str):
    """
    Reads a sample of the CSV from GCS and generates semantic column descriptions
    using a language model. This is used to enrich the BigQuery schema.
    """
    from google.cloud import storage
    import io
    import pandas as pd
    
    storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Read first 10KB to get header and some rows
    content = blob.download_as_bytes(start=0, end=10240).decode('utf-8')
    df = pd.read_csv(io.StringIO(content), nrows=5)
    
    sample_data = df.to_string()
    columns = df.columns.tolist()
    
    # Use the root_agent's model to generate descriptions
    # We'll use a simple prompt for the generation
    prompt = (
        f"Analyze these CSV columns and sample data:\n\n"
        f"Columns: {columns}\n"
        f"Sample Data:\n{sample_data}\n\n"
        "Generate a JSON object where keys are column names and values are "
        "concise, professional descriptions of what the data represents in a "
        "finance context. Output ONLY the JSON."
    )
    
    # Since we are inside a tool, we can use the agent's model if we had access, 
    # but more reliably we use a fresh GenerativeModel call.
    from google.generativeai import GenerativeModel
    model = GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)
    
    try:
        # Clean up response in case it has markdown blocks
        clean_json = response.text.replace('```json', '').replace('```', '').strip()
        metadata = json.loads(clean_json)
        
        # Save locally for the ingestion tool to pick up
        meta_path = os.path.join(os.path.dirname(__file__), 'schema_metadata.json')
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        return {
            "status": "success",
            "message": f"Generated metadata for {len(columns)} columns.",
            "metadata": metadata
        }
    except Exception as e:
        return {"error": f"Failed to parse or save metadata: {str(e)}", "raw_response": response.text}

# MCP Wrapper for ADK/External use
mcp = FastMCP("Finance Ingestion Tools")
mcp.add_tool(search_for_existing_finance_data)
mcp.add_tool(ingest_from_gcs)
mcp.add_tool(generate_schema_metadata)

# 4. Define the ADK Agent
root_agent = Agent(
    name="sharechat_finance_agent",
    instruction=(
        f"You are a Finance Data Analyst for project '{PROJECT_ID}'. "
        "Your first priority is to ensure data is available in BigQuery for analysis while avoiding redundant ingestion."
        "\n\nFOLLOW THIS WORKFLOW:"
        "\n1. Ask the user for the GCS bucket and CSV file name."
        "\n2. Run 'search_for_existing_finance_data' to check if suitable data already exists."
        "\n3. If no suitable data exists, run 'generate_schema_metadata' to analyze the file and create a schema mapping."
        "\n4. Use 'ingest_from_gcs' to load the file and apply the generated metadata."
        "\n5. Use 'execute_sql' for analysis, focusing on 'cost_usd', 'usage_type', and 'region'."
    ),
    tools=[bq_toolset, ingest_from_gcs, search_for_existing_finance_data, generate_schema_metadata]
)

if __name__ == "__main__":
    mcp.run()
