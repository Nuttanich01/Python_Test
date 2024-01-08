
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from scripts.ge_movie_rating import GEMovieRatingService
from scripts.ingestion import CSVIngestion
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 
import great_expectations as gx

class GEMOvieRatingService:
    @staticmethod
    def csv_pre_process_checking(**kwargs):
        expectation_suite = kwargs.get('ex')
        datasource = kwargs.get('datasource')
        path = kwargs.get('path')
        run_id = kwargs.get('run_id')

        baseic_checking(path,'path',expectation_suite,datasource,run_id)

def baseic_checking(source, source_type, expectation_suite, datasource, run_id):
    context = gx.DataContext()
    suite = context.get_expectation_suite(expectation_suite)
    batch_kwargs = {source_type: source,
                    'datasoutce': datasource}
    batch = context.get_batch(batch_kwargs, suite)
    result = context.run_validation_operator("action_list_operator"
                                             , assets_to_validate=[batch], run_id=run_id)
    if not result["success"]:
        raise Exception('there is an unexpected case in datasets')

with DAG(
        dag_id='Test_airflow_2',
        start_date=days_ago(0),
        schedule_interval="@daily",
        tags=["Test"]
) as dag:
    
    csv_file_pre_process_checking_id = 'csv_file_pre_process_checking'
    csv_file_pre_process_checking = PythonOperator(
        task_id=csv_file_pre_process_checking_id,
        provide_context=True,
        python_callable= GEMovieRatingService.csv_pre_process_checking,
        op_kwargs={'expectation_suite': 'Test_data',
                   'datasource': 'movie_rating_dir',
                   'path': '/opt/airflow/data/Real_data.cs',
                   'run_id': csv_file_pre_process_checking_id}
    )

    csv_file_pre_process_checking_id