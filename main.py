from google.auth import default
from google.cloud import bigquery
import requests
import pandas as pd
from datetime import datetime
import os

# Google Cloud Setup
CREDENTIALS, PROJECT_ID = default()
DATASET_ID = "DataSF_Project"
TABLE_ID = "real_estate_transactions"

# Connect to BigQuery
bq_client = bigquery.Client(credentials=CREDENTIALS, project=PROJECT_ID)

# Get the latest date in BigQuery
query = f"""
    SELECT MAX(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', data_loaded_at)) AS latest_data_loaded_at
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE data_loaded_at IS NOT NULL
"""

query_job = bq_client.query(query)
latest_result = query_job.result()
latest_data_loaded_at = None

for row in latest_result:
    latest_data_loaded_at = row["latest_data_loaded_at"]

# Convert latest_data_loaded_at to the correct API format
if latest_data_loaded_at:
    latest_data_loaded_at_str = latest_data_loaded_at.strftime('%Y-%m-%dT%H:%M:%S')
    API_URL = f"https://data.sfgov.org/resource/wv5m-vpq2.json?$limit=50000&$where=data_loaded_at>'{latest_data_loaded_at_str}'"
else:
    API_URL = "https://data.sfgov.org/resource/wv5m-vpq2.json?$limit=50000"

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

# Load into BigQuery (append new data)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
df.to_gbq(table_ref, project_id=PROJECT_ID, if_exists="append", credentials=CREDENTIALS)

print(f"Inserted {len(df)} new records into BigQuery table: {table_ref}")