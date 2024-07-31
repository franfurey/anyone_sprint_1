from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import json

# Definir el DAG y sus argumentos
default_args = {
    'owner': 'franciscofurey',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 29),
    'retries': 1,
}

dag = DAG(
    dag_id='taskflow_dag',
    default_args=default_args,
    description='A simple TaskFlow DAG',
    schedule_interval=None,  # Ejecutar bajo demanda
)

@task(dag=dag)
def extract():
    """Extract task: Load data from a string"""
    data_string = '{"1001": 301.27, "1002": 433.21}'
    order_data_dict = json.loads(data_string)
    return order_data_dict

@task(dag=dag)
def transform(order_data_dict: dict):
    """Transform task: Calculate total order value"""
    total_order_value = sum(order_data_dict.values())
    return {"total_order_value": total_order_value}

# Definir el flujo de tareas
order_data = extract()
order_summary = transform(order_data)
