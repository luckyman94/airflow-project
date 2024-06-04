from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys
import pandas as pd

HOME = "/opt/airflow"
DATALAKE_ROOT_FOLDER = HOME + "/data/"

sys.path.append(HOME)


def train_model_and_get_recommendations():
    from src.machine_learning.model import RecommendationModel
    df = pd.read_parquet(DATALAKE_ROOT_FOLDER + "final_dataset.parquet")
    rm = RecommendationModel(df)
    rm.generate_recommendations_df(
        ["Mekhong Full Moon Party", "Il était une fois la révolution", "Little Big Man", "Pour une poignée de dollars",
         "Gleboka woda", "HOW TO BUILD A GIRL"])


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

with DAG(
        'train_model',
        default_args=default_args,
        description='A simple DAG to train model',
        schedule_interval=None,
        catchup=False,
        tags=["training"]
) as dag:
    dag.doc_md = """"
           This is the DAG I used to run my Data Project in airflow.
           I can write documentation in Markdown here with *bold text* or _bold text_."""

task_train_model = PythonOperator(
    task_id='train_model_generate_predictions',
    python_callable=train_model_and_get_recommendations,
    dag=dag,
)

task_train_model