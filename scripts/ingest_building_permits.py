import logging
from google.auth import default
from google.cloud import bigquery
import requests
import pandas as pd
import pandas_gbq
import sys

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Google Cloud Setup
CREDENTIALS, PROJECT_ID = default()
DATASET_ID = "DataSF_Project"
TABLE_ID = "building_permits"

# Connect to BigQuery
bq_client = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)

# Get the latest `data_loaded_at` timestamp from BigQuery
query = f"""
    SELECT MAX(
        COALESCE(
            SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', data_loaded_at),
            SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', data_loaded_at)
        )
    ) AS latest_data_loaded_at
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE data_loaded_at IS NOT NULL
"""
query_job = bq_client.query(query)
latest_data_loaded_at = query_job.result().to_dataframe()["latest_data_loaded_at"].iloc[0]

# Handle case where `latest_data_loaded_at` is None
if latest_data_loaded_at and not pd.isna(latest_data_loaded_at):
    latest_data_loaded_at_str = latest_data_loaded_at.strftime('%Y-%m-%dT%H:%M:%S')
    API_URL = f"https://data.sfgov.org/resource/i98e-djp9.json?$limit=100000&$where=data_loaded_at>'{latest_data_loaded_at_str}'"
else:
    API_URL = "https://data.sfgov.org/resource/i98e-djp9.json?$limit=100000"

# Fetch Data from API
response = requests.get(API_URL)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)

    if df.empty:
        logging.info("No new data found. Skipping insertion.")
        sys.exit(0)

    logging.info(f"Fetched {len(df)} new records from the API.")
else:
    logging.error(f"Error fetching data: {response.status_code}")
    sys.exit(1)

# Data Cleaning
df = df.where(pd.notnull(df), None)

# Ensure column types match BigQuery
if "data_loaded_at" in df.columns:
    df["data_loaded_at"] = pd.to_datetime(df["data_loaded_at"], errors="coerce").astype(str)  # Convert for BQ compatibility

# **Deduplicate new records before inserting**
df.drop_duplicates(subset=[
    "permit_number", "permit_type", "permit_type_definition", "permit_creation_date", "filed_date",
    "issued_date", "status", "street_number", "street_name", "street_suffix", "zipcode",
    "neighborhoods_analysis_boundaries", "number_of_proposed_stories", "proposed_units",
    "existing_units", "estimated_cost", "revised_cost", "description", "data_as_of"
], inplace=True)

logging.info(f"After deduplication: {len(df)} rows remaining.")

# If no new records remain, exit
if df.empty:
    logging.info("No new data to insert after deduplication.")
    sys.exit(0)

# Fetch existing schema from BigQuery
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
table_schema = bq_client.get_table(table_ref).schema
existing_columns = {field.name for field in table_schema}

# Filter only known columns before inserting
df = df[[col for col in df.columns if col in existing_columns]]

pandas_gbq.to_gbq(df, table_ref, project_id=PROJECT_ID, if_exists="append", credentials=CREDENTIALS)

logging.info(f"Inserted {len(df)} new records into BigQuery table: {table_ref}")