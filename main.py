import requests
import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import os

# Google Cloud Setup
PROJECT_ID = "splendid-light-431320-d8"
DATASET_ID = "DataSF_Project"
TABLE_ID = "real_estate_transactions"

# Load Service Account Credentials
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/app/credentials.json")

from google.oauth2 import service_account
CREDENTIALS = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

# DataSF API Endpoint (Fetching up to 50,000 rows)
API_URL = "https://data.sfgov.org/resource/wv5m-vpq2.json?$limit=50000"

# Fetch Data from API
response = requests.get(API_URL)
if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)
    print(f"Fetched {len(df)} records from the API.")
else:
    print(f"Error fetching data: {response.status_code}")
    exit()

# Data Cleaning & Transformations
df = df.where(pd.notnull(df), None)  # Convert NaN to None

# Add `data_loaded_at` column for tracking updates
df["data_loaded_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Define BigQuery Table
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Write DataFrame to BigQuery (Auto-Detect Schema)
pandas_gbq.to_gbq(df, table_ref, project_id=PROJECT_ID, if_exists="replace", credentials=CREDENTIALS)

print(f"Inserted {len(df)} records into BigQuery table: {table_ref}")