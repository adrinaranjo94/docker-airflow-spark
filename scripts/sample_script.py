from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("ExampleApp").getOrCreate()
    data = [("John", 28), ("Smith", 45), ("Sara", 23)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    df.show()

if __name__ == "__main__":
    main()
