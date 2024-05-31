from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
HOME = "/opt/airflow"
DATALAKE_ROOT_FOLDER = HOME + "/data/"

sys.path.append(HOME)

from src.scraping.allocine import run_scrap_allocine
from src.scraping.netflix import download_netflix_data

def upload_to_s3():
    from src.utils.s3_manager import S3Manager
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
        description='A simple DAG to fetch data',
        schedule_interval=None,
        catchup=False,
        tags=["scraping"]
) as dag:
    dag.doc_md = """"
           This is the DAG I used to run my Data Project in airflow.
           I can write documentation in Markdown here with *bold text* or _bold text_."""

task_scrap_netflix = PythonOperator(
        task_id='scrap_netflix',
        python_callable=download_netflix_data,
        dag=dag,
    )

task_scrap_allocine = PythonOperator(
        task_id='scrap_allocine',
        python_callable=run_scrap_allocine,
        dag=dag,
        op_kwargs={
            "num_pages":1
        }

    )

task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3

    )


task_scrap_netflix >> task_scrap_allocine >> task_upload_to_s3

