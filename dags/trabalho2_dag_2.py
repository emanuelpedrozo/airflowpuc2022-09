from operator import index
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': "Emanuel",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 15)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['DAG2'])
def trabalho2_dag2():
    @task
    def media_total():
        NOME_TABELA = "/tmp/resultados.csv"
        file = r"/tmp/tabela_unica.csv"
        df = pd.read_csv(file, sep=";")
        res1 = df.groupby(['Sex'])['PassengerId', 'Fare', 'sibsp_parch'].mean().reset_index()
        print(res1)
        res1.to_csv(NOME_TABELA, index=False, sep=';')

    fim = EmptyOperator(task_id="fim")

    indicador = media_total()

    indicador >> fim

execucao = trabalho2_dag2()