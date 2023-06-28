## General Functions
from prefect import flow, task
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import random
import psycopg2
from connections.bigquery_connection import connect_to_bigquery
from google.cloud import bigquery
from google.api_core import retry
import datetime

#Funcion de generacion de datos sinteticos
@task
def generate_dataframe():
    # Configuración de las variables
    num_rows = 100

    categorical_variables = {
        'multiplelines': {'classes': ['No', 'Yes', 'No phone service'], 'distribution': [48, 42, 10]},
        'internetservice': {'classes': ['Fiber optic', 'DSL', 'No'], 'distribution': [44, 34, 22]},
        'onlinesecurity': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [50, 28, 22]},
        'onlinebackup': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [44, 34, 22]},
        'deviceprotection': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [44, 34, 22]},
        'techsupport': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [50, 30, 20]},
        'streamingtv': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [40, 40, 20]},
        'streamingmovies': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [40, 40, 20]},
        'contract': {'classes': ['Month-to-month', 'One year', 'Two year'], 'distribution': [55, 25, 20]},
        'paymentmethod': {'classes': ['Electronic check', 'Mailed check', 'Bank transfer (automatic)', 'Credit card (automatic)'], 'distribution': [34, 23, 22, 21]},
        'gender': {'classes': ['Male', 'Female'], 'distribution': [50, 50]},
        'paperlessbilling': {'classes': ['No', 'Yes'], 'distribution': [40, 60]},
        'partner': {'classes': ['No', 'Yes'], 'distribution': [55, 45]},
        'dependents': {'classes': ['No', 'Yes'], 'distribution': [70, 30]},
        'phoneservice': {'classes': ['No', 'Yes'], 'distribution': [20, 80]},
        'seniorcitizen': {'classes': ['0', '1'], 'distribution': [80, 20]}

    }


    numeric_variables = {
        'monthlycharges': {'min': 20, 'mean': 65, 'max': 110},
        'totalcharges': {'min': 18, 'mean': 1397, 'max': 8000},
        'tenure': {'min': 10, 'mean': 30, 'max': 60},

    }

    # Generación de los datos
    data = {}

    idies = np.array([str(''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=6)))
                 for _ in range(num_rows)])
    
    data['clienteidentifier'] = idies

    for variable, info in categorical_variables.items():
        classes = info['classes']
        distribution = info['distribution']
        data[variable] = np.random.choice(classes, size=num_rows, p=[p/100 for p in distribution])


    for variable, info in numeric_variables.items():
        min_val = info['min']
        mean_val = info['mean']
        max_val = info['max']
        data[variable] = np.random.normal(loc=mean_val, scale=(max_val - min_val) / 6, size=num_rows)
        data[variable] = np.clip(data[variable], min_val, max_val)
    
    current_date = datetime.date.today()
    data['fecha_ingreso'] = current_date

    df = pd.DataFrame(data)
    df['clienteidentifier'] = df['clienteidentifier'].astype(str)
    df['multiplelines'] = df['multiplelines'].astype(str)
    df['internetservice'] = df['internetservice'].astype(str)
    df['onlinesecurity'] = df['onlinesecurity'].astype(str)
    df['onlinebackup'] = df['onlinebackup'].astype(str)
    df['deviceprotection'] = df['deviceprotection'].astype(str)
    df['techsupport'] = df['techsupport'].astype(str)
    df['streamingtv'] = df['streamingtv'].astype(str)
    df['streamingmovies'] = df['streamingmovies'].astype(str)
    df['contract'] = df['contract'].astype(str)
    df['paymentmethod'] = df['paymentmethod'].astype(str)
    df['gender'] = df['gender'].astype(str)
    df['paperlessbilling'] = df['paperlessbilling'].astype(str)
    df['partner'] = df['partner'].astype(str)
    df['dependents'] = df['dependents'].astype(str)
    df['phoneservice'] = df['phoneservice'].astype(str)
    df['seniorcitizen'] = df['seniorcitizen'].astype(str)
    df['monthlycharges'] = df['monthlycharges'].astype(float)
    df['totalcharges'] = df['totalcharges'].astype(float)
    df['tenure'] = df['tenure'].astype(float)
    df['fecha_ingreso'] = pd.to_datetime(df['fecha_ingreso']).dt.date
    

    return df

## Funcion de Ingesta del dataframe a Postgresql
@task
def ingest_or_create_to_bigquery(df, table_name, dataset_id):
    #configurar cliente de bigqury

    client = connect_to_bigquery()
    table_ref = client.dataset(dataset_id).table(table_name)

    # Verificar si la tabla existe
   # table_exists = client.get_table(table_ref, retry=retry.Retry(deadline=30))
    
    # Verificar si la tabla existe
    table_exists = client.get_table(table_ref)
    
    if table_exists is None:
        # Crear la tabla si no existe
        table = bigquery.Table(table_ref)
        schema = []
        for column_name, column_type in df.dtypes.items():
            schema.append(bigquery.SchemaField(name=column_name, field_type=column_type.name))
        table.schema = schema
        table = client.create_table(table)
        print(f"Se ha creado la tabla {dataset_id}.{table_name} en BigQuery.")
    
    # Cargar los datos del DataFrame en la tabla
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


    """#cargar datos:
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
    """

@flow(name="Synt churn data generator x Bigquery")
def generate_churn_data_bigquery():
    table_name = 'churn_last'
    dataset_id = 'ml_datasets'
    df= generate_dataframe()
    load_completed = ingest_or_create_to_bigquery(df, table_name, dataset_id)
    return load_completed

if __name__ == '__main__':
 generate_churn_data_bigquery()