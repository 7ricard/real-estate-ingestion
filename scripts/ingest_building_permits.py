from google.auth import default
from google.cloud import bigquery
import requests
import pandas as pd
from datetime import datetime

# Google Cloud Setup
CREDENTIALS, PROJECT_ID = default()
DATASET_ID = "DataSF_Project"
TABLE_ID = "building_permits"

# Connect to BigQuery
bq_client = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)

# Get the latest data_loaded_at timestamp from BigQuery
query = f"""
    SELECT MAX(
        COALESCE(
            SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', data_loaded_at),
            SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', data_loaded_at)
        )
    ) AS latest_data_loaded_at
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE data_loaded_at IS NOT NULL
"""
query_job = bq_client.query(query)
latest_result = query_job.result()
latest_data_loaded_at = None

for row in latest_result:
    latest_data_loaded_at = row["latest_data_loaded_at"]

# Debugging log
print(f"Latest data_loaded_at from BigQuery: {latest_data_loaded_at}")

# Convert to API format
latest_data_loaded_at_str = None
if latest_data_loaded_at:
    latest_data_loaded_at_str = latest_data_loaded_at.strftime('%Y-%m-%dT%H:%M:%S')

# Construct API URL
if latest_data_loaded_at_str:
    API_URL = f"https://data.sfgov.org/resource/i98e-djp9.json?$limit=100000&$where=data_loaded_at>'{latest_data_loaded_at_str}'"
else:
    API_URL = "https://data.sfgov.org/resource/i98e-djp9.json?$limit=100000"

# Fetch Data from API
response = requests.get(API_URL)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)

    if df.empty:
        print("No new data found. Skipping insertion.")
        exit()
    
    print(f"Fetched {len(df)} new records from the API.")
else:
    print(f"Error fetching data: {response.status_code}")
    exit()

# Data Cleaning
df = df.where(pd.notnull(df), None)

# DO NOT overwrite `data_loaded_at`
# Preserve `data_loaded_at` from API

# Load into BigQuery (append new data)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
df.to_gbq(table_ref, project_id=PROJECT_ID, if_exists="append", credentials=CREDENTIALS)

print(f"Inserted {len(df)} new records into BigQuery table: {table_ref}")