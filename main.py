## General Functions
from prefect import flow, task
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import random
import psycopg2
#Funcion de generacion de datos sinteticos
@task
def generate_dataframe():
    # Configuración de las variables
    num_rows = 100

    categorical_variables = {
        'MultipleLines': {'classes': ['No', 'Yes', 'No phone service'], 'distribution': [48, 42, 10]},
        'InternetService': {'classes': ['Fiber optic', 'DSL', 'No'], 'distribution': [44, 34, 22]},
        'OnlineSecurity': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [50, 28, 22]},
        'OnlineBackup': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [44, 34, 22]},
        'DeviceProtection': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [44, 34, 22]},
        'TechSupport': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [50, 30, 20]},
        'StreamingTV': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [40, 40, 20]},
        'StreamingMovies': {'classes': ['No', 'Yes', 'No internet service'], 'distribution': [40, 40, 20]},
        'Contract': {'classes': ['Month-to-month', 'One year', 'Two year'], 'distribution': [55, 25, 20]},
        'PaymentMethod': {'classes': ['Electronic check', 'Mailed check', 'Bank transfer (automatic)', 'Credit card (automatic)'], 'distribution': [34, 23, 22, 21]},
        'gender': {'classes': ['Male', 'Female'], 'distribution': [50, 50]}
    }

    boolean_variables = {
        'Partner': {'values': ['No', 'Yes'], 'distribution': [55, 45]},
        'Dependents': {'values': ['No', 'Yes'], 'distribution': [70, 30]},
        'PhoneService': {'values': ['No', 'Yes'], 'distribution': [20, 80]},
        'SeniorCitizen': {'values': [0, 1], 'distribution': [80, 20]},
        'PaperlessBilling': {'values': ['No', 'Yes'], 'distribution': [40, 60]}
    }

    numeric_variables = {
        'MonthlyCharges': {'min': 20, 'mean': 65, 'max': 110},
        'TotalCharges': {'min': 18, 'mean': 1397, 'max': 8000},
        'tenure': {'min': 10, 'mean': 30, 'max': 60}
    }

    # Generación de los datos
    data = {}

    # Generación de la columna 'customerID'
    data['customerID'] = [''.join(random.choices('0123456789', k=4)) + '-' +
                          ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=4)) +
                          ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
                          for _ in range(num_rows)]

    for variable, info in categorical_variables.items():
        classes = info['classes']
        distribution = info['distribution']
        data[variable] = np.random.choice(classes, size=num_rows, p=[p/100 for p in distribution])

    for variable, info in boolean_variables.items():
        values = info['values']
        distribution = info['distribution']
        data[variable] = np.random.choice(values, size=num_rows, p=[p/100 for p in distribution])

    for variable, info in numeric_variables.items():
        min_val = info['min']
        mean_val = info['mean']
        max_val = info['max']
        data[variable] = np.random.normal(loc=mean_val, scale=(max_val - min_val) / 6, size=num_rows)
        data[variable] = np.clip(data[variable], min_val, max_val)

    
    # Creación del DataFrame
    df = pd.DataFrame(data)

    return df

## Funcion de Ingesta del dataframe a Postgresql
@task
def ingest_to_postgresql(df, table_name, connection_string):
    # Establecer una conexión con la base de datos
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    # Crear un motor de SQLAlchemy para insertar el DataFrame
    engine = create_engine(connection_string)

    # Insertar el DataFrame en la tabla existente sin borrar los datos antiguos
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    # Confirmar y cerrar la conexión
    conn.commit()
    cursor.close()
    conn.close()


@flow(name="Synt churn data generator ")
def generate_churn_data():
    table_name = 'churn_sync_data'
    connection_string = 'postgresql://postgres:Bolochoww-44@localhost:5432/postgres'
    df = generate_dataframe()
    load_completed = ingest_to_postgresql(df, table_name, connection_string)
    return load_completed

if __name__ == '__main__':
 generate_churn_data()