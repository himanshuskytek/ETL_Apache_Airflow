from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
# from custom_oracle_operator import CustomOracleOperator
from airflow.operators.oracle_operator import OracleOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'oracle_to_airflow',
    default_args=default_args,
    description='A DAG to fetch data from Oracle to Apache Airflow',
    schedule_interval='@daily',
)
# SELECT imdb_movie_id
# FROM sqlmdb.movies
# ORDER BY COALESCE(last_updated, first_created);
# FETCH FIRST 10 ROWS ONLY;
def fetch_from_oracle():
    oracle_task = OracleOperator(
        task_id='fetch_from_oracle',
        oracle_conn_id='oracle_test',
        sql='SELECT imdb_movie_id FROM sqlmdb.movies',
        dag=dag,
    )
    oracle_task.execute(context=None)

fetch_task = PythonOperator(
    task_id='fetch_task',
    python_callable=fetch_from_oracle,
    dag=dag,
)

fetch_task
