from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Definir el DAG
default_args = {
    'owner': 'franciscofurey',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 29),
    'retries': 1,
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,  # Ejecutar bajo demanda
)

# Definir una tarea usando BashOperator
t1 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello World"',
    dag=dag,
)
