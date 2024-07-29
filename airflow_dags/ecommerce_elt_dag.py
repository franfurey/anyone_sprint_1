# src.ecommerce_elt_dag.py
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
# from src.extract import extract
# from src.load import load
# from src.transform import transform

# Definición del default_args
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
with DAG(
    'ecommerce_elt_pipeline',
    default_args=default_args,
    description='DAG for E-Commerce ELT pipeline',
    schedule_interval=timedelta(days=1),  # Running the DAG on a daily basis
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Tareas de Extracción
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract  # función definida en src/extract.py
    )

    # Tareas de Carga
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load  # función definida en src/load.py
    )

    # Tareas de Transformación
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=2  # función definida en src/transform.py
    )

    # Define el flujo de las tareas
    extract_task >> load_task >> transform_task

