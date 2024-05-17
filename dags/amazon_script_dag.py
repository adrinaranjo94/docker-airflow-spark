from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from sqlalchemy import create_engine
import pandas as pd

from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

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
        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        # Send a GET request to the Amazon URL
        response = requests.get(url, headers=headers)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content of the page
            soup = BeautifulSoup(response.text, 'html.parser')
            
             # Print the parsed HTML content for debugging
            print(soup.prettify())

            # Find all elements with the price class
            prices = soup.find_all(class_='_1lyGfBIWIsBkBm5lMbuH2D')
            
            # Extract the text content of each price element
            prices_list = [price.get_text(strip=True) for price in prices]
            
            return prices_list
        else:
            print("Failed to fetch Amazon page. Status code:", response.status_code, response)
            return None
    except Exception as e:
        print("An error occurred:", str(e))
        return None

def process_amazon_prices(**kwargs):
    # Example URL from the provided web
    url = "https://moviesanywhere.com/explore/deals"

    # Scrape prices from the Amazon page
    prices = scrape_amazon_prices(url)
    if prices:
        print("Prices:", prices)
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        
        df = pd.DataFrame({'Price': prices})

        df.to_sql('prices_table', engine, if_exists='append', index=False)
        df.to_csv('scraped_prices.csv', index=False)

        # You can do further processing or store the prices here
    else:
        print("Failed to scrape prices from the Amazon page.")
    

dag = DAG(
    'amazon_price_scraping_dag',
    default_args=default_args,
    description='A DAG to scrape prices from Amazon',
    schedule_interval=timedelta(days=1),
)

scrape_task = PythonOperator(
    task_id='scrape_amazon_prices_task',
    python_callable=process_amazon_prices,
    dag=dag,
)
