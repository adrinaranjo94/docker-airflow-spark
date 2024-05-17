from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def scrape_amazon_prices(url):
    try:
        # Send a GET request to the Amazon URL
        response = requests.get(url)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content of the page
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all elements with the price class
            prices = soup.find_all(class_='_1lyGfBIWIsBkBm5lMbuH2D')
            
            # Extract the text content of each price element
            prices_list = [price.get_text(strip=True) for price in prices]
            
            return prices_list
        else:
            print("Failed to fetch Amazon page. Status code:", response.status_code)
            return None
    except Exception as e:
        print("An error occurred:", str(e))
        return None

def process_with_spark():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AmazonPriceProcessing") \
        .getOrCreate()
    
    # Example URL from the provided web
    url = "https://moviesanywhere.com/explore/deals"
    
    # Scrape prices from the Amazon page
    prices = scrape_amazon_prices(url)
    if prices:
        # Convert the list of prices to a DataFrame
        df = spark.createDataFrame([(price,) for price in prices], ['price'])
        
        # Perform any desired processing or analysis using Spark DataFrame operations
        # For example, you can count the number of prices
        price_count = df.count()
        print("Number of scraped prices:", price_count)
        
        # You can also perform additional processing or analysis here
        
        # Stop Spark session
        spark.stop()
    else:
        print("Failed to scrape prices from the Amazon page.")

dag = DAG(
    'amazon_price_scraping_dag',
    default_args=default_args,
    description='A DAG to scrape prices from Amazon and process with Spark',
    schedule_interval=timedelta(days=1),
)

scrape_task = PythonOperator(
    task_id='scrape_amazon_prices_task',
    python_callable=scrape_amazon_prices,
    op_kwargs={'url': 'https://moviesanywhere.com/explore/deals'},
    dag=dag
)

spark_task = PythonOperator(
    task_id='process_amazon_prices_spark_task',
    python_callable=process_with_spark,
    dag=dag
)

scrape_task >> spark_task
