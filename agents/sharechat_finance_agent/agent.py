import google.auth
from google.adk import Agent
from google.adk.tools.bigquery import BigQueryToolset, BigQueryCredentialsConfig
from mcp.server.fastmcp import FastMCP
import os
import json
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

# 1. Configuration & Credentials
PROJECT_ID = "shashirr01"
BUCKET_NAME = "shashi148"
FINANCE_DATASET_ID = "sharechat_finance_data"
MODEL_NAME = "gemini-3.1-pro-preview"
credentials, _ = google.auth.default()

# 2. BigQuery Toolset (ADK Connector)
bq_toolset = BigQueryToolset(
    credentials_config=BigQueryCredentialsConfig(credentials=credentials)
)

# 3. Custom Tools for Ingestion, Visualization, and Reporting
def get_dataset_inventory():
    """
    Scans the 'sharechat_finance_data' dataset to list all tables, their row counts, 
    and shared columns (correlations) to enable multi-table JOINs.
    """
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    dataset_ref = client.dataset(FINANCE_DATASET_ID)
    
    try:
        tables = list(client.list_tables(dataset_ref))
    except NotFound:
        return {"message": f"Dataset {FINANCE_DATASET_ID} is empty. Please ingest a file first."}

    inventory = []
    all_columns = {}

    for table_item in tables:
        table = client.get_table(table_item.reference)
        cols = {field.name.lower() for field in table.schema}
        all_columns[table.table_id] = cols
        inventory.append({
            "table_id": table.table_id,
            "row_count": table.num_rows,
            "columns": list(cols)
        })

    correlations = []
    table_names = list(all_columns.keys())
    for i in range(len(table_names)):
        for j in range(i + 1, len(table_names)):
            t1, t2 = table_names[i], table_names[j]
            common = all_columns[t1].intersection(all_columns[t2])
            if common:
                correlations.append({
                    "tables": [t1, t2],
                    "common_columns": list(common),
                    "join_hint": f"FROM `{PROJECT_ID}.{FINANCE_DATASET_ID}.{t1}` JOIN `{PROJECT_ID}.{FINANCE_DATASET_ID}.{t2}` ON " + " AND ".join([f"{t1}.{c} = {t2}.{c}" for c in common])
                })

    return {
        "dataset": FINANCE_DATASET_ID,
        "available_tables": inventory,
        "correlations": correlations
    }

from google.genai import types
from google.adk.tools import ToolContext

async def generate_finance_chart(sql_query: str, chart_type: str, title: str, x_label: str, y_label: str, tool_context: ToolContext):
    """
    Executes a SQL query, retrieves data, and generates a visual chart.
    The chart is saved as an artifact and displayed in the UI.
    """
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import pandas as pd
    import io
    from google.cloud import bigquery
    
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    try:
        df = client.query(sql_query).to_dataframe()
    except Exception as e:
        return {"error": f"SQL execution failed: {str(e)}"}
    
    if df.empty:
        return {"error": "No data found for the given query."}

    # Robust numeric conversion
    if len(df.columns) >= 2:
        val_col = df.columns[1]
        df[val_col] = pd.to_numeric(df[val_col], errors='coerce')
        df = df.dropna(subset=[val_col])
    
    if df.empty:
        return {"error": "The query returned no numeric data suitable for plotting."}

    plt.figure(figsize=(10, 6))
    try:
        if chart_type.lower() == "bar":
            df.plot(kind='bar', x=df.columns[0], y=df.columns[1], legend=False)
        elif chart_type.lower() == "line":
            df.plot(kind='line', x=df.columns[0], y=df.columns[1], marker='o')
        elif chart_type.lower() == "pie":
            plt.pie(df[df.columns[1]], labels=df[df.columns[0]], autopct='%1.1f%%')
        else:
            plt.close()
            return {"error": "Unsupported chart type."}
    except Exception as e:
        plt.close()
        return {"error": f"Plotting failed: {str(e)}"}
    
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    img_bytes = buf.read()
    buf.close()
    plt.close()
    
    filename = f"chart_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.png"
    artifact_part = types.Part(inline_data=types.Blob(mime_type="image/png", data=img_bytes))
    
    # Save artifact - this puts it in actions.artifact_delta which ADK Web UI renders
    await tool_context.save_artifact(filename=filename, artifact=artifact_part)
    
    return {
        "status": "success",
        "message": f"Chart '{title}' generated and saved as artifact '{filename}'."
    }

def format_finance_report(sql_query: str, output_format: str):
    """
    Executes a SQL query and formats the result as a CSV, JSON, or Markdown table.
    Useful for cross-table reporting.
    """
    import pandas as pd
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    try:
        df = client.query(sql_query).to_dataframe()
    except Exception as e:
        return {"error": f"SQL execution failed: {str(e)}"}
    
    if df.empty:
        return {"error": "No data found for the given query."}
    
    if output_format.lower() == "csv":
        return {"format": "csv", "content": df.to_csv(index=False)}
    elif output_format.lower() == "json":
        return {"format": "json", "content": df.to_json(orient='records', indent=2)}
    elif output_format.lower() == "markdown":
        return {"format": "markdown", "content": df.to_markdown(index=False)}
    else:
        return {"error": "Unsupported format. Use 'csv', 'json', or 'markdown'."}

def ingest_from_gcs(file_name: str, table_id: str):
    """
    Ingests a CSV from the hardcoded bucket 'shahsi148' into BigQuery.
    """
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    dataset_ref = client.dataset(FINANCE_DATASET_ID)
    table_ref = dataset_ref.table(table_id)
    
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)

    try:
        client.get_table(table_ref)
        return {"error": f"Table '{table_id}' already exists."}
    except NotFound:
        pass

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        skip_leading_rows=1,
    )
    uri = f"gs://{BUCKET_NAME}/{file_name}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    
    # Apply Metadata
    table = client.get_table(table_ref)
    meta_path = os.path.join(os.path.dirname(__file__), 'schema_metadata.json')
    if os.path.exists(meta_path):
        with open(meta_path, 'r') as f:
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

    return {"status": "success", "table_path": f"{PROJECT_ID}.{FINANCE_DATASET_ID}.{table_id}"}

def generate_schema_metadata(file_name: str):
    """
    Updates the semantic registry for a new file in bucket 'shahsi148'.
    """
    from google.cloud import storage
    import io
    import pandas as pd
    
    storage_client = storage.Client(credentials=credentials, project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    
    content = blob.download_as_bytes(start=0, end=20480).decode('utf-8', errors='ignore')
    df = pd.read_csv(io.StringIO(content), nrows=10)
    
    prompt = (
        f"Analyze these columns: {df.columns.tolist()}\n"
        f"Sample: {df.head(2).to_string()}\n"
        "Return JSON {column: description}."
    )
    
    from google.generativeai import GenerativeModel
    model = GenerativeModel(MODEL_NAME)
    response = model.generate_content(prompt)
    
    try:
        clean_json = response.text.replace('```json', '').replace('```', '').strip()
        new_meta = json.loads(clean_json)
        meta_path = os.path.join(os.path.dirname(__file__), 'schema_metadata.json')
        existing_meta = {}
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                existing_meta = json.load(f)
        existing_meta.update(new_meta)
        with open(meta_path, 'w') as f:
            json.dump(existing_meta, f, indent=2)
        return {"status": "success", "mapped": list(new_meta.keys())}
    except Exception as e:
        return {"error": str(e)}

# MCP Wrapper
mcp = FastMCP("Finance Ingestion & Viz Tools")
mcp.add_tool(get_dataset_inventory)
mcp.add_tool(ingest_from_gcs)
mcp.add_tool(generate_schema_metadata)
# Removed generate_finance_chart from MCP as it uses ADK-specific ToolContext
mcp.add_tool(format_finance_report)

# 4. Define the ADK Agent
root_agent = Agent(
    name="sharechat_finance_agent",
    instruction=(
        f"You are an Advanced Finance Data Analyst for project '{PROJECT_ID}' using model '{MODEL_NAME}'."
        f"Bucket: '{BUCKET_NAME}', Dataset: '{FINANCE_DATASET_ID}'."
        "\n\nCAPABILITIES & WORKFLOW:"
        "\n1. ANALYZE: For any query, first run 'get_dataset_inventory' to map tables and shared columns."
        "\n2. VISUALIZE: Use 'generate_finance_chart' for trends or comparisons. "
        "The tool automatically saves the chart as an artifact which the UI will render. You just need to acknowledge the chart has been generated."
        "\n3. REPORT: Use 'format_finance_report' with 'csv', 'json', or 'markdown' for structured cross-table reports."
        "\n4. MULTI-TABLE: Automatically perform JOINs across tables using correlations from inventory."
        "\n5. INGEST: Use 'generate_schema_metadata' then 'ingest_from_gcs' for new GCS files."
        "\n6. PROACTIVE: When asked for 'a report on growth', don't just return numbers; generate a chart and a markdown summary table."
    ),
    tools=[bq_toolset, ingest_from_gcs, get_dataset_inventory, generate_schema_metadata, generate_finance_chart, format_finance_report]
)

if __name__ == "__main__":
    mcp.run()
