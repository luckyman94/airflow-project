import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

HOME = "/opt/airflow"
DATALAKE_ROOT_FOLDER = HOME + "/data/"

sys.path.append(HOME)

def clean_data():
    from src.utils.directory_manager import DirectoryManager
    dm = DirectoryManager()
    dm.remove_files_with_extension(DATALAKE_ROOT_FOLDER,".csv")
    dm.clean_empty_subdirectories(DATALAKE_ROOT_FOLDER)

def format_data():
    from src.format_file.file_formatter import FileFormatter
    from src.utils.s3_manager import S3Manager

    s3_manager = S3Manager()
    formatter = FileFormatter()

    csv_files = s3_manager.list_csv_files_in_bucket()
    local_parquet_file = None

    for csv_file in csv_files:
        local_csv_path = os.path.join(DATALAKE_ROOT_FOLDER, os.path.basename(csv_file))
        s3_manager.download_file(csv_file, local_csv_path)
        s3_manager.delete_file(csv_file)

        df = formatter.read_csv(local_csv_path, header=True)
        local_parquet_dir = os.path.join(DATALAKE_ROOT_FOLDER, os.path.splitext(os.path.basename(csv_file))[0])
        formatter.convert_to_parquet(df, local_parquet_dir)

        for file_name in os.listdir(local_parquet_dir):
            if file_name.endswith(".parquet"):
                local_parquet_file = os.path.join(local_parquet_dir, file_name)
                break

        if local_parquet_file is not None:
            s3_manager.upload_file(os.path.splitext(csv_file)[0] + ".parquet", local_parquet_file)

    formatter.stop()


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
        'formatter',
        default_args=default_args,
        description='A simple DAG to convert csv files into parquet',
        schedule_interval=None,
        catchup=False,
        tags=["preprocessing", "formatter"]
) as dag:
    dag.doc_md = """"
           This is the DAG I used to run my Data Project in airflow.
           I can write documentation in Markdown here with *bold text* or _bold text_."""


task_format_data = PythonOperator(
        task_id='format_data',
        python_callable=format_data,
        dag=dag,
    )

task_clean_data_dir = PythonOperator(
    task_id='clean_data_dir',
    python_callable=clean_data,
    dag=dag
)


task_format_data >> task_clean_data_dir
