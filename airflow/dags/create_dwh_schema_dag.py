import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

import helpers

default_args = {
    "owner": 'brutway',
    "start_date": datetime.now(),
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1
}

dag = DAG('create_dwh_schema_dag',
    default_args    = default_args,
    description     = "Create DWH schema",
    schedule_interval = "@once"
)

def exec_query(**kwargs):
    """
    Execute queries from a list one by one
    :param kwargs:
    :return:
    """
    logging.info("Execute list of queries")
    dwh_conn_id = kwargs["dwh_conn_id"]
    list_of_queries = kwargs["list_of_queries"]

    db_hook = PostgresHook(dwh_conn_id)
    for query in list_of_queries:
        logging.info("Exec query from the list: " + query)
        db_hook.run(query)

drop_dwh_schema = PythonOperator(
    task_id = "drop_dwh_schema",
    dag = dag,
    python_callable = exec_query,
    op_kwargs = { "dwh_conn_id": "dwh", "list_of_queries": helpers.drop_schema_queries }
)

create_dwh_schema = PythonOperator(
    task_id = "create_dwh_schema",
    dag = dag,
    python_callable = exec_query,
    op_kwargs = { "dwh_conn_id": "dwh", "list_of_queries": helpers.create_schema_queries }
)

create_stg_tables = PythonOperator(
    task_id = "create_stg_tables",
    dag = dag,
    python_callable = exec_query,
    op_kwargs = { "dwh_conn_id": "dwh", "list_of_queries": helpers.create_stg_table_queries }
)

create_dwh_tables = PythonOperator(
    task_id = "create_dwh_tables",
    dag = dag,
    python_callable = exec_query,
    op_kwargs = { "dwh_conn_id": "dwh", "list_of_queries": helpers.create_dds_table_queries }
)

create_dm_tables = PythonOperator(
    task_id = "create_dm_tables",
    dag = dag,
    python_callable = exec_query,
    op_kwargs = { "dwh_conn_id": "dwh", "list_of_queries": helpers.create_dm_table_queries }
)

create_etl_tables = PythonOperator(
    task_id = "create_etl_tables",
    dag = dag,
    python_callable = exec_query,
    op_kwargs = { "dwh_conn_id": "dwh", "list_of_queries": helpers.create_etl_table_queries }
)

start_operator = DummyOperator(
    task_id = 'Begin_execution',
    dag = dag
)

end_operator = DummyOperator(
    task_id = 'Stop_execution',
    dag = dag
)

start_operator >> drop_dwh_schema >> create_dwh_schema

create_dwh_schema >> create_stg_tables >> end_operator
create_dwh_schema >> create_dwh_tables >> end_operator
create_dwh_schema >> create_dm_tables >> end_operator
create_dwh_schema >> create_etl_tables >> end_operator
