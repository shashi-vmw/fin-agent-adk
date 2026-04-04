# ShareChat Finance Agent (ADK + BigQuery)

This agent is a specialized **Finance Data Analyst** built using the **Google Agent Development Kit (ADK)** and integrated with **Google BigQuery** and **Google Cloud Storage (GCS)**. It enables natural language querying over massive financial datasets (tested up to 800MB) while maintaining high cost-efficiency through redundant data detection.

---

## 🚀 Key Features

- **Large Dataset Support:** Optimized for 100MB+ CSV files by offloading local files to GCS before BigQuery ingestion.
- **Redundant Ingestion Prevention:** Once a user specifies a file, the agent automatically scans the project for existing tables with similar schemas and prompts the user to reuse them.
- **Semantic Metadata Enrichment:** Automatically maps column descriptions from `schema_metadata.json` to BigQuery table schemas during ingestion, enhancing the accuracy of SQL generation.
- **Hosted Deployment:** Fully compatible with **GCP Agent Engine (Vertex AI Reasoning Engine)** with native `google-adk` framework identification.

---

## 🏗️ Architecture

1.  **Ingestion Engine:** Uses the `google-cloud-bigquery` Python SDK to manage data transfers from GCS.
2.  **Redundancy Discovery Tool:** A custom tool that searches all datasets for tables containing financial indicators like `cost_usd`, `usage_type`, and `amount`.
3.  **BigQuery Toolset:** Leverages ADK's native `BigQueryToolset` for secure, high-performance SQL generation and execution.
4.  **Semantic Mapping:** Uses a local JSON registry to provide human-readable descriptions for BigQuery columns, critical for the LLM's understanding of financial metrics.

---

## 📂 Project Structure

```text
/
├── agents/
│   └── sharechat_finance_agent/
│       ├── agent.py               # Main ADK Agent logic & custom tools
│       ├── schema_metadata.json   # Column descriptions for BQ schemas
│       └── __init__.py            # Package entry point
├── GEMINI.md                      # Design document
└── README.md                      # This documentation
```

---

## 🛠️ Deployment Steps

To deploy this agent to **GCP Agent Engine (Vertex AI)**, follow these steps:

### 1. Prerequisites
- Google Cloud CLI (`gcloud`) installed and authenticated.
- Vertex AI and BigQuery APIs enabled.
- Python 3.11 environment with ADK installed:
  ```bash
  pip install google-adk google-cloud-aiplatform[agent_engines,adk]
  ```

### 2. Prepare the Agent Package
Ensure your agent is located in `agents/sharechat_finance_agent/`. The ADK CLI expects this structure.

### 3. Deploy via ADK CLI
Run the following command from the project root:

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"

adk deploy agent_engine 
    --project="your-project-id" 
    --region="us-central1" 
    --display_name="ShareChat Finance Agent" 
    agents/sharechat_finance_agent
```

**Why use the ADK CLI?** 
It automatically handles:
- Wrapping the agent in `AdkApp`.
- Correctly setting the **Framework** column to `google-adk` in the Vertex AI console.
- Creating the necessary staging buckets and packaging dependencies.

---

## 🔍 How to Use

Once deployed, you can interact with the agent via the Vertex AI Console or API.

**Example Inquiries:**
- *"I have a file at gs://my-bucket/usage_data.csv. Can you ingest it and compare compute costs between APAC and EMEA for the last 3 months?"*
- *"Identify users whose storage costs exceed $500/month in the latest ingested table."*
- *"What is our total global cost normalized to USD?"*

**Note on Background Check:**
The agent will always ask for your CSV details first. Once provided, it will silently check if the data is already in BigQuery and notify you if a match is found before starting a new load job.

---

## 🧪 Local Development & Testing

You can run the agent's MCP server locally for testing tools:

```bash
cd agents/sharechat_finance_agent
python3 agent.py
```

Or run the interactive CLI:
```bash
adk run agents/sharechat_finance_agent
```

---

## 🔒 Security
- **Hardcoded Project IDs:** The agent is configured for `shashirr01`. Update `agent.py` and deployment scripts if moving to a different project.
- **Service Account Permissions:** Ensure the service account running the agent has `roles/bigquery.dataEditor`, `roles/storage.objectAdmin`, and `roles/aiplatform.user`.
