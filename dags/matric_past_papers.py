from airflow.decorators import dag,task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.models.baseoperator import chain
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table ,Metadata
from datetime import datetime
import requests
from include.tasks import verify_links,upload_files_to_s3


BUCKET_NAME ='matric-past-papers-raw'




# Define Dag


@dag(
    start_date = datetime(2024, 1,1),
    schedule_interval='@daily',
    catchup =False,
    tags =['matric-past-exam-papers-pipeline']

)


def matric_pipeline():
    
    # Task 1: Verify links
    check_links = PythonOperator(
        task_id="check_links",
        python_callable=verify_links,
        provide_context=True,
    )

    # Task 2: to download files
    get_load_files_to_s3= PythonOperator(
        task_id="get_load_files_to_s3",
        python_callable = upload_files_to_s3,
        provide_context=True,
        op_kwargs={
            'bucket_name': BUCKET_NAME,  
            'aws_connection': 'AWS_Connection',  
        }
    )



    check_links >> get_load_files_to_s3

matric_pipeline()


