import logging
import configparser
import boto3
from datetime import datetime
from pathlib import Path


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import helpers

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/review_project.cfg"))

target_bucket = config.get("S3_STORAGE", "LANDING_ZONE")

default_args = {
    "owner": 'brutway',
    "start_date": datetime.now(),
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1
}

s3_resource = boto3.resource("s3",
                             region_name = config.get("AWS", "REGION"),
                             aws_access_key_id = config.get("AWS", "KEY"),
                             aws_secret_access_key = config.get("AWS", "SECRET")
                             )

def copy_review_data(*args, **kwargs):
    """
    Copy reviews from Amazon Customer Reviews Dataset to Landing Zone for further processing
    :param args:
    :param kwargs: Product_Category filter
    :return:
    """
    source_bucket = config.get("S3_DATA_SOURCE", "BUCKET_REVIEW")

    file_list = []
    for category in helpers.product_category_filter.get(kwargs["product_category_filter"]):
        prefix = f"parquet/product_category={category}/"
        for obj in  s3_resource.Bucket(source_bucket).objects.filter(Prefix = prefix):
            file_list.append(obj.key)

    for file in file_list:
        logging.info(f"File {file}")
        s3_resource.meta.client.copy({'Bucket': source_bucket, 'Key': file}, target_bucket, "review/" + file)

def copy_country_data(*args, **kwargs):
    """
    Copy country dataset from S3 bucket to Landing Zone for further processing
    :param args:
    :param kwargs:
    :return:
    """
    source_bucket = config.get("S3_DATA_SOURCE", "BUCKET_COUNTRY")

    file_list = []
    prefix = f"country/"
    for obj in  s3_resource.Bucket(source_bucket).objects.filter(Prefix = prefix):
        file_list.append(obj.key)

    for file in file_list:
        logging.info(f"File {file}")
        s3_resource.meta.client.copy({'Bucket': source_bucket, 'Key': file}, target_bucket, file)

def clear_landing_zone(*args, **kwargs):
    """
    Clear Landing Zone before copying data from sources
    :param args:
    :param kwargs:
    :return:
    """
    s3_resource.Bucket(target_bucket).objects.all().delete()


dag = DAG('copy_initial_data_dag',
    default_args    = default_args,
    description     = "Copy initial data from sources to Landing Zone",
    schedule_interval = "@once"
)

clear_landing = PythonOperator(
    task_id = "clear_landing_zone",
    python_callable = clear_landing_zone,
    dag = dag
)

copy_review = PythonOperator(
    task_id = "copy_review_data",
    python_callable = copy_review_data,
    op_kwargs = {"product_category_filter": config.get("INITIAL_LOADING", "REVIEW_INITIAL_DATA")},
    provide_context = True,
    dag = dag
)

copy_country = PythonOperator(
    task_id = "copy_country_data",
    python_callable = copy_country_data,
    dag = dag
)

start_operator = DummyOperator(
    task_id = "Begin_execution",
    dag = dag
)

end_operator = DummyOperator(
    task_id = "Stop_execution",
    dag = dag
)

start_operator >> clear_landing
clear_landing >> copy_review >> end_operator
clear_landing >> copy_country >> end_operator