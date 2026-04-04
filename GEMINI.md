# ShareChat Finance Agent Design (ADK + BQ)

## Overview
This agent is designed to ingest large financial CSV files (up to 800MB) and enable natural language querying via Gemini Enterprise and Google BigQuery.

## Key Components

### 1. Ingestion Tool (ADK-based)
- **Path:** `bq_data_loader.py`
- **Functionality:** 
  - Offloads large files to GCS for efficient BQ loading.
  - Automatically maps column descriptions using `schema_metadata.json`.
  - Ensures the BigQuery Agent has the necessary semantic context for SQL generation.

### 2. Semantic Mapping (`schema_metadata.json`)
- Stores human-readable descriptions for each column.
- **Why?** BQ Agents rely heavily on column descriptions to generate accurate SQL. Without this, the agent might confuse `amount` (usage) with `cost_usd` (monetary).

### 3. BigQuery Agent Integration
- Once data is loaded and metadata is populated, the user can query the dataset in natural language.
- **Example Inquiries:**
  - "Compare compute costs between APAC and EMEA for the last 3 months."
  - "Identify users whose storage costs exceed $500/month."
  - "What is our total global cost normalized to USD?"

## Deployment on GCP Agent Engine
1. Register the `load_data_with_metadata` function as a tool in ADK.
2. Configure the agent with a BigQuery Dataset connection.
3. Grant the service account `roles/bigquery.dataEditor` and `roles/storage.objectAdmin` permissions.
