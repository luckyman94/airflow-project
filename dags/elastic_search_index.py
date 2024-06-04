import os
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

HOME = "/opt/airflow"
DATALAKE_ROOT_FOLDER = HOME + "/data/"

sys.path.append(HOME)

def index_data():
    from src.indexing.elastic_search_indexer import ElasticsearchIndexer
    indexer = ElasticsearchIndexer()
    indexer.test_connection()
    #indexer.index_from_parquet(DATALAKE_ROOT_FOLDER+"recommendations.parquet")


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
        'indexer',
        default_args=default_args,
        description='A simple DAG to index recommendations to elastic search',
        schedule_interval=None,
        catchup=False,
        tags=["indexing"]
) as dag:
    dag.doc_md = """"
           This is the DAG I used to run my Data Project in airflow.
           I can write documentation in Markdown here with *bold text* or _bold text_."""


task_index_data = PythonOperator(
        task_id='index_data',
        python_callable=index_data,
        dag=dag,
    )


task_index_data
