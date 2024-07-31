from datetime import datetime, timedelta
from airflow.decorators import dag, task
import sys
import os

# Asegurarse de que el directorio src está en el PATH para poder importar los módulos de Python
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Importar los módulos personalizados
from extract import extract
from load import load, main as load_main
from transform import run_queries

# Configuración de default_args para el DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 7, 31),
}

# Definición del DAG usando el decorador 'dag'
@dag(default_args=default_args, schedule_interval='@daily', catchup=False, tags=['ecommerce'])
def ecommerce_elt_pipeline():
    @task()
    def extract_task():
        """Extrae datos usando el módulo de extracción."""
        csv_folder = '/path/to/csv/folder'  # Ajustar según la estructura de tu proyecto
        csv_table_mapping = {
            'olist_customers_dataset.csv': 'customers',
            'olist_geolocation_dataset.csv': 'geolocation',
            # Añadir todos los mapeos necesarios aquí
        }
        public_holidays_url = 'https://date.nager.at/api/v2/publicholidays'
        extracted_data = extract(csv_folder=csv_folder, csv_table_mapping=csv_table_mapping, public_holidays_url=public_holidays_url)
        return extracted_data

    @task()
    def load_task(extracted_data):
        """Carga los datos extraídos en la base de datos."""
        from sqlalchemy import create_engine
        database_url = 'sqlite:///path_to_your_database.db'  # Ajustar a la configuración de tu base de datos
        engine = create_engine(database_url)
        load(data_frames=extracted_data, database=engine)

    @task()
    def transform_task():
        """Transforma los datos ya cargados utilizando SQL y los almacena para su uso."""
        from sqlalchemy import create_engine
        database_url = 'sqlite:///path_to_your_database.db'  # Ajustar a la configuración de tu base de datos
        engine = create_engine(database_url)
        transformed_data = run_queries(database=engine)
        return transformed_data

    # Definir el flujo de tareas
    extracted_data = extract_task()
    load_task(extracted_data=extracted_data)
    transformed_data = transform_task()

# Asignar el objeto DAG al contexto global para que Airflow lo detecte
dag = ecommerce_elt_pipeline()
