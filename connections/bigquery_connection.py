# bigquery_connection.py
from google.cloud import bigquery
from google.oauth2 import service_account

def connect_to_bigquery():
    # Load BigQuery credentials from the service_account.json file
    credentials = service_account.Credentials.from_service_account_file('C:/Users/diego/OneDrive/Escritorio/mlops_projects/ml/churn_syn_data_generator_pipeline/connections/protean-fabric-386717-d6a21dd66382.json')

    # Connect to the BigQuery API using the credentials
    client = bigquery.Client(credentials=credentials)
    
    return client
