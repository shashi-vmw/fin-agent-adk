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

# MCP Wrapper for ADK/External use
mcp = FastMCP("Finance Ingestion Tools")
mcp.add_tool(search_for_existing_finance_data)
mcp.add_tool(ingest_from_gcs)

# 4. Define the ADK Agent
root_agent = Agent(
    name="sharechat_finance_agent",
    instruction=(
        f"You are a Finance Data Analyst for project '{PROJECT_ID}'. "
        "Your first priority is to ensure data is available in BigQuery for analysis while avoiding redundant ingestion."
        "\n\nFOLLOW THIS WORKFLOW:"
        "\n1. Ask the user for the GCS bucket and CSV file name if they haven't provided it yet."
        "\n2. Once the CSV details are provided, immediately run 'search_for_existing_finance_data' "
        "to check if a table with a similar schema already exists in the project."
        "\n3. If a matching table is found, inform the user and ask if they would like to use the existing data instead of ingesting the CSV again."
        "\n4. If no suitable data exists or the user wants a fresh load, use 'ingest_from_gcs' to load the file."
        "\n5. Use 'execute_sql' for analysis, focusing on 'cost_usd', 'usage_type', and 'region'."
    ),
    tools=[bq_toolset, ingest_from_gcs, search_for_existing_finance_data]
)

if __name__ == "__main__":
    mcp.run()
