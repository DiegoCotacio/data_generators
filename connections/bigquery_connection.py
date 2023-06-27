# bigquery_connection.py
from google.cloud import bigquery
from google.oauth2 import service_account
import os

def connect_to_bigquery():

    # Load BigQuery credentials from the secret
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    # Load BigQuery credentials from the service_account.json file
    credentials = service_account.Credentials.from_service_account_info(credentials_json)

    # Connect to the BigQuery API using the credentials
    client = bigquery.Client(credentials=credentials)
    
    return client
