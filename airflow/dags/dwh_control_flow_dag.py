from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    "owner": 'brutway',
    "start_date": datetime.now(),
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1
}

dag = DAG('DWH_processing_dag',
    default_args    = default_args,
    description     = "Processing data in the Redshift Database",
    schedule_interval = "@once"
#    schedule_interval = "0 7 0 0 0"
)

emr_ssh_hook = SSHHook(ssh_conn_id = "emr_ssh_connection")

process_storage_job = SSHOperator(
    task_id = "process_S3_storage",
    command = 'cd /home/hadoop/review_project_src;export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;spark-submit --master yarn storage_control_flow.py;',
    ssh_hook = emr_ssh_hook,
    dag = dag
)

process_dwh_job = SSHOperator(
    task_id = "process_DWH",
    command = 'cd /home/hadoop/review_project_src;export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;spark-submit --master yarn dwh_control_flow.py;',
    ssh_hook = emr_ssh_hook,
    dag = dag
)


start_operator = DummyOperator(
    task_id = 'Begin_execution',
    dag = dag
)

end_operator = DummyOperator(
    task_id = 'Stop_execution',
    dag = dag
)

start_operator >> process_storage_job
process_storage_job >> process_dwh_job >> end_operator


