from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import cx_Oracle

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'oracle_conn_viacode',
    default_args=default_args,
    description='A DAG to fetch data from Oracle to Apache Airflow',
    schedule_interval='@daily',
)

def fetch_from_oracle():
    # Construct the connection string using SID
    dsn = cx_Oracle.makedsn('reade.forest.usf.edu', '1521', sid='cdb9')

    # Establish connection
    connection = cx_Oracle.connect(user='SQL116', password='SKY@8tek', dsn=dsn)
    cursor = connection.cursor()

    # Execute SQL query
    cursor.execute('SELECT imdb_movie_id FROM sqlmdb.movies')
    result = cursor.fetchall()

    # Close cursor and connection
    cursor.close()
    connection.close()

    print(result)

fetch_task = PythonOperator(
    task_id='fetch_task',
    python_callable=fetch_from_oracle,
    dag=dag,
)

fetch_task
