import logging
import pandas as pd
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": 'brutway',
    "start_date": datetime.now(),
    "email_on_failure": False,
    "email_on_retry": False,
    "max_active_runs": 1
}

dag = DAG('fill_dimensions_dag',
    default_args    = default_args,
    description     = "Fill dimensions tables: Calendar, Rating",
    schedule_interval = "@once"
)

def fill_calendar(**kwargs):
    """
    Fill in dim_calendar table in the Data Warehouse
    :param kwargs:
    :return:
    """
    db_hook = PostgresHook(kwargs["dwh_conn_id"])
    conn = db_hook.get_conn()
    cur = conn.cursor()

    query = """truncate table dds.dim_calendar;"""
    cur.execute(query)
    conn.commit()

    query = """
    insert into dds.dim_calendar(cdate,	day, day_of_week, day_of_week_name,	month, month_name, month_short_name, year)
    with digit as (
        select 0 as d union all 
        select 1 union all select 2 union all select 3 union all
        select 4 union all select 5 union all select 6 union all
        select 7 union all select 8 union all select 9        
    ),
    seq as (
        select a.d + (10 * b.d) + (100 * c.d) + (1000 * d.d) + (10000 * e.d) as num
        from digit a
            cross join
            digit b
            cross join
            digit c
            cross join
            digit d
            cross join
            digit e
        order by 1        
    ),
    dates as 
    (
        select date '1995-01-01' + num as cdate
        from seq
        where date '1995-01-01' + num <= date '2024-12-31'
    )
    select cdate
        , to_char(cdate, 'DD') as day
        , to_char(cdate, 'ID')::smallint as day_of_week
        , to_char(cdate, 'Day') as day_of_week_name
        , to_char(cdate, 'MM') as month
        , to_char(cdate, 'Month') as month_name
        , to_char(cdate, 'Mon') as month_short_name
        , extract('year' from cdate) as year
    from dates
    ;
    """
    cur.execute(query)
    conn.commit()

def fill_rating(**kwargs):
    """
    Fill in dim_rating table in the Data Warehouse
    :param kwargs:
    :return:
    """
    db_hook = PostgresHook(kwargs["dwh_conn_id"])
    conn = db_hook.get_conn()
    cur = conn.cursor()

    query = """truncate table dds.dim_rating;"""
    cur.execute(query)
    conn.commit()

    query = """
    insert into dds.dim_rating (star_rating, rating_title)  
    select *
    from (
        select 1 as star_rating, 'Very bad' as rating_title
        union all
        select 2 as star_rating, 'Bad' as rating_title
        union all
        select 3 as star_rating, 'Not good' as rating_title
        union all
        select 4 as star_rating, 'Good' as rating_title
        union all
        select 5 as star_rating, 'VeryExcellent' as rating_title
    ) r;
    """
    cur.execute(query)
    conn.commit()

create_calendar = PythonOperator(
    task_id = "fill_calendar",
    dag = dag,
    python_callable = fill_calendar,
    op_kwargs = { "dwh_conn_id": "dwh" }
)

create_rating = PythonOperator(
    task_id = "fill_rating",
    dag = dag,
    python_callable = fill_rating,
    op_kwargs = { "dwh_conn_id": "dwh" }
)
start_operator = DummyOperator(
    task_id = 'Begin_execution',
    dag = dag
)

end_operator = DummyOperator(
    task_id = 'Stop_execution',
    dag = dag
)

start_operator >> create_calendar >> end_operator
start_operator >> create_rating >> end_operator