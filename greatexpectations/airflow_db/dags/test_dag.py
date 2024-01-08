from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 
import pandas as pd
import glob
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
import great_expectations as gx
from airflow.operators.dummy_operator import DummyOperator

context = gx.get_context()
# Connect to Data
# validator = context.sources.pandas_default.read_csv("/opt/airflow/data/Real_data.csv")

with DAG(
        dag_id='Test_airflow',
        start_date=days_ago(0),
        schedule_interval="@daily",
        tags=["Test"]
) as dag:
    

    task1 = DummyOperator(task_id='task1', dag=dag)
    task2 = DummyOperator(task_id='task2', dag=dag)
    task4 = DummyOperator(task_id='task4', dag=dag)

    task3 = GreatExpectationsOperator(
    task_id="task3",
    data_context_root_dir='/opt/airflow/great_expectations',
    checkpoint_name='my_checkpoint',
    return_json_dict = True,
    dag=dag,
)

task1 >> task2 >> task3 >> task4
