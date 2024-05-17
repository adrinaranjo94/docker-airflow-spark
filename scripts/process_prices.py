from pyspark.sql import SparkSession

def process_prices(prices):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AmazonPriceProcessing") \
        .getOrCreate()

    # Create DataFrame from list of prices
    df = spark.createDataFrame([(price,) for price in prices], ["price"])

    # Perform processing (e.g., calculate average price)
    average_price = df.groupBy().avg("price").collect()[0][0]

    print("Average Price:", average_price)

    spark.stop()

if __name__ == "__main__":
    # Example list of prices (replace with actual scraped prices)
    prices = [10.99, 20.50, 15.75, 30.25]

    # Process prices using PySpark
    process_prices(prices)
