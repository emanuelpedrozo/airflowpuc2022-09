from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'Ney',
    'dependes_on_past': False,
    'start_date': datetime(2022, 9, 29)
}

dag = DAG(
    "exercicio-01",
    default_args = default_args,
    description = "Primeiro exercicio no Airflow",
    schedule_interval = "*/1 * * * *",
    tags = ['PUC', 'PRE', 'Hello'],
    catchup = False
)

hello_bash = BashOperator(
    task_id = "hello_do_bash",
    bash_command = "echo 'Hello World do Bash... que lindinho...'",
    dag = dag
)

# Fazer a definição do método Python que vai ser executado
def hello_world():
    print("Hello World do Python... Super elegante...")

hello_python = PythonOperator(
    task_id = "hello_do_python",
    python_callable = hello_world,
    dag=dag
)

# Orquestrar
hello_python >> hello_bash
