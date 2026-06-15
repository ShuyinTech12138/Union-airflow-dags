from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_connection():
    hook = MsSqlHook(mssql_conn_id='mssql_local28')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('SELECT 1')
    print('Connection successful!')

with DAG(
    dag_id='test_mssql_connection',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    test = PythonOperator(
        task_id='test_query',
        python_callable=test_connection,
    )