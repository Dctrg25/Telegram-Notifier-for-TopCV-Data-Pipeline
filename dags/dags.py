from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from dags.Extract.extract import extract
from dags.Transform.transform_data import transform_data
from dags.Load.load_postgres import Load_schema

default_args = {
    'owner': 'dctrg',
    'start_date': datetime(2023, 10, 14),
    'retries' : 3,
    'retry_delay' : timedelta(minutes=2),
    'email_on_retry' : False,
    'depends_on_past' : False
}

dag = DAG('ETL_topcv_postgres_dag',
          default_args= default_args,
          description= "crawl data from topcv and load to postgres",
          schedule= '@daily'
          )

extract_data = PythonOperator(
    task_id= 'extract',
    python_callable= extract,
    dag= dag
)

transfrom = PythonOperator(
    task_id= 'transform',
    python_callable= transform_data,
    dag= dag
)

call_procedure = PostgresOperator(
    task_id= 'call_procedure',
    postgres_conn_id= 'postgres_job_db',
    sql= 'SELECT update_due_time();',
    dag= dag
)

load = PythonOperator(
    task_id= 'load',
    python_callable= Load_schema,
    dag= dag
)

extract_data >> transfrom >> call_procedure >> load