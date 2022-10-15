import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args = {
    'owner': "Emanuel",
    "depends_on_past": False,
    'start_date': datetime (2022, 10, 15)
}

@dag(default_args=default_args, schedule_interval='@once', catchup=False, tags=['DAG1'])
def trabalho02_dag01():

    @task
    def cap_dados():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, sep=";")
        return NOME_DO_ARQUIVO

    @task
    def ind_passageiros(nome_do_arquivo):
        NOME_TABELA = "/tmp/passageiros_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({
            "PassengerId": "count"
        }).reset_index()
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=";")
        return NOME_TABELA

    @task
    def taf_passageiros(nome_do_arquivo):
        NOME_TABELA_2 = "/tmp/tarifa_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        res = df.groupby(['Sex', 'Pclass']).agg({
            "Fare": "mean"
        }).reset_index()
        print(res)
        res.to_csv(NOME_TABELA_2, index=False, sep=";")
        return NOME_TABELA_2

    @task
    def ind_sisb_parch(nome_do_arquivo):
        NOME_TABELA_3 = "/tmp/sibsp_parch_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=";")
        df['sibsp_parch'] = df['SibSp'] + df['Parch']
        res = df.groupby(['Sex', 'Pclass']).agg({
            "sibsp_parch": "count"
        }).reset_index()
        print(res)
        res.to_csv(NOME_TABELA_3, index=False, sep=";")
        return NOME_TABELA_3

    @task
    def get_together(NOME_TABELA, NOME_TABELA_2, NOME_TABELA_3):
        TABELA_UNICA = "/tmp/tabela_unica.csv"
        df = pd.read_csv(NOME_TABELA, sep=";")
        df1 = pd.read_csv(NOME_TABELA_2, sep=";")
        df2 = pd.read_csv(NOME_TABELA_3, sep=";")
        df3 = df.merge(df1, on=['Sex', 'Pclass'], how='inner')
        df4 = df2.merge(df3, on=['Sex', 'Pclass'], how='inner')
        return TABELA_UNICA
   
    inicio = DummyOperator(task_id="inicio")
    #fim = DummyOperator(task_id="fim")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def end():
        print("Terminou")
    triggerdag = TriggerDagRunOperator(
        task_id="trigga_trabalho_02_ex02",
        trigger_dag_id="trabalho2_dag_2")

    df = cap_dados()
    indicador = ind_passageiros(df) 
    ticket = taf_passageiros(df)
    sibsp_parch = ind_sisb_parch(df)
    tab_final = get_together(indicador, ticket, sibsp_parch)

    inicio >> df >> [indicador, ticket, sibsp_parch] >> tab_final >> triggerdag

execucao = trabalho02_dag01()
        