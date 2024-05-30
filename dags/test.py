import os
from datetime import date, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys



sys.path.append("/opt/airflow")




HOME = "/opt/airflow"
DATALAKE_ROOT_FOLDER = HOME + "/data/"



def download_netflix(**kwargs):
    from src.netflix_downloader import download_netflix_data
    if not os.path.exists(DATALAKE_ROOT_FOLDER):
        os.makedirs(DATALAKE_ROOT_FOLDER)

    download_netflix_data()

def upload_to_s3():
    from src.s3_manager import S3Manager
    s3_manager = S3Manager()
    s3_manager.upload_directory(DATALAKE_ROOT_FOLDER,  remove_files=True, extension='.csv')


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
        's3_dag',
        default_args=default_args,
        description='A simple DAG to fetch IMDb data',
        schedule_interval=None,
        catchup=False,
) as dag:
    dag.doc_md = """"
           This is the DAG I used to run my Data Project in airflow.
           I can write documentation in Markdown here with *bold text* or _bold text_."""

task_scrap_netflix = PythonOperator(
        task_id='scrap_netflix',
        python_callable=download_netflix,
        dag=dag
    )

task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3

    )


task_scrap_netflix >> task_upload_to_s3

