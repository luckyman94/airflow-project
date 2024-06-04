import os
import sys
HOME = "/opt/airflow"
DATALAKE_ROOT_FOLDER = HOME + "/data/"
sys.path.append(HOME)
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.utils.s3_manager import S3Manager
from src.concat_data.concatenate import Concatenate

def clean_data_dir():
    from src.utils.directory_manager import DirectoryManager
    dm = DirectoryManager()
    dm.clean_directory_with_wrong_name(DATALAKE_ROOT_FOLDER, "final_dataset.parquet")

def upload_to_s3():
    s3_manager = S3Manager()
    s3_manager.upload_directory(DATALAKE_ROOT_FOLDER, s3_directory="combined_data")

def concat_data():
    cc = Concatenate()
    print("Begin processing netflix")
    cc.preprocess_netflix(export_parquet=True)
    print("End processing netflix")

    print("Begin processing allocine")
    cc.preprocess_allocine(export_parquet=True)
    print("End processing allocine")

    print("Combining data")
    cc.combine_data()


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
        'concatenate',
        default_args=default_args,
        description='A simple DAG to combine data',
        schedule_interval=None,
        catchup=False,
        tags=["preprocessing"]
) as dag:
    dag.doc_md = """"
           This is the DAG I used to run my Data Project in airflow.
           I can write documentation in Markdown here with *bold text* or _bold text_."""

task_concat_dataset = PythonOperator(
        task_id='concat_data',
        python_callable=concat_data,
        dag=dag,
    )

task_clean_data_dir = PythonOperator(
        task_id='clean_data_dir',
        python_callable=clean_data_dir,
        dag=dag,
    )

task_upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_to_s3,
        dag=dag,
    )


task_concat_dataset >> task_clean_data_dir >> task_upload_data
