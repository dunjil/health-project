# Dag will run here
from datetime import datetime, timedelta
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePipelineOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator, DataflowConfiguration
from airflow.providers.google.cloud.hooks.gcs import GCSHook
# pip install apache-airflow-providers-apache-beam


GCP_PROJECT_ID = 'banded-lexicon-459415-q2'
GCP_LOCATION = 'us-east1'
TEMP_LOCATION = 'gs://bucket-radic-healthcare/temp/'
STAGING_LOCATION = 'gs://bucket-radic-healthcare/staging/'
ETL_SCRIPTS_PATH = 'gs://bucket-radic-healthcare/etl/'

default_args = {
'owner': '10alytics',
'depends_on_past': False,
'start_date': datetime(2025, 1, 1),
'retries': 1,
'retry_delay': timedelta(minutes=1),

}



def get_sql_from_gcs(**context):
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bucket_name = 'bucket-du-health-project'
    file_name = 'sql/create_star_schema.sql'
    file_content = gcs_hook.download_as_byte_array(bucket_name=bucket_name,object_name= file_name).decode('utf-8')
    return file_content


with models.DAG(
    dag_id='health_etl_dag',
    default_args=default_args,
    description='Daily Health Tech Dag That Fetches Data from a Postgres DB and Loads it into BigQuery',
    schedule_interval=timedelta(days=1),
) as dag:
    start = EmptyOperator(task_id='start')

    # Task to fetch SQL from GCS
    fetch_sql = PythonOperator(
        task_id='fetch_sql',
        python_callable=get_sql_from_gcs,
        provide_context=True,
    )

    # Task to create star schema in BigQuery
    create_star_schema = BigQueryInsertJobOperator(
        task_id='create_star_schema',
        configuration={
            "query": {
                "query": "{{ task_instance.xcom_pull(task_ids='fetch_sql') }}",
                "useLegacySql": False,
            }
        },
        location='us-east1',
        project_id='banded-lexicon-459415-q2',
    )

    etl_tasks = []
    for etl_script in [
        'diagnosis_etl',
        'encounters_etl',
        'facilities_etl',
        'patients_etl',
        'providers_etl'
    ]:
        dataflow_task = BeamRunPythonPipelineOperator(
            task_id=f'run_{etl_script}',
            py_file=f"{ETL_SCRIPTS_PATH}{etl_script}.py",
            dataflow_config=DataflowConfiguration(job_name=f"{etl_script}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",project_id=GCP_PROJECT_ID, location=GCP_LOCATION,service_account="radic-dataflow-sa@radic-healthcare.iam.gserviceaccount.com",wait_until_finished=True),
            gcp_conn_id='google_cloud_default',
            runner='DataflowRunner',
            pipeline_options={
                "tempLocation": TEMP_LOCATION,
                "stagingLocation": STAGING_LOCATION,
                "project": GCP_PROJECT_ID,
                "region": GCP_LOCATION,
            },
            # py_requirements=['apache-beam[gcp]'],  # Add your dependencies here
            py_interpreter='python3',
            py_system_site_packages=False,
        )
        etl_tasks.append(dataflow_task)


    end = EmptyOperator(task_id='end')

    start >> fetch_sql >> create_star_schema >> etl_tasks >> end
