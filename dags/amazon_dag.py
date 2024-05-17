from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator  # Adjusted import


from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'amazon_price_scraper',
    default_args=default_args,
    description='A DAG to scrape prices from Amazon and process using Spark',
    schedule_interval=timedelta(days=1),
)

# Task to scrape prices from Amazon using a Python script
scrape_prices_task = BashOperator(
    task_id='scrape_prices',
    bash_command='python /opt/airflow/scripts/scrape_amazon_prices.py',
    dag=dag,
)

# Task to process the scraped data using Spark
process_prices_task = SparkSubmitOperator(
    task_id='process_prices',
    application='/opt/airflow/scripts/process_prices.py',
    conn_id='spark_default',  # Make sure this corresponds to your Spark connection
    name='Process Amazon Prices',
    verbose=False,
    dag=dag,
)

scrape_prices_task >> process_prices_task
