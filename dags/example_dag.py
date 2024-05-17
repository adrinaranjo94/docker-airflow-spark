from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def submit_spark_job():
    os.system(f'/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/submit.py')

run_this_first = BashOperator(
    task_id='run_this_first',
    bash_command='echo "Starting the DAG"',
    dag=dag,
)

run_spark = PythonOperator(
    task_id='run_spark',
    python_callable=submit_spark_job,
    dag=dag,
)

run_this_first >> run_spark
