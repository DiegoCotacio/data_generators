# Importar funciones y clases
from .bigquery_connection import connect_to_bigquery

# Definir funciones globales del paquete
connect = {
    "connect_to_bigquery": connect_to_bigquery}