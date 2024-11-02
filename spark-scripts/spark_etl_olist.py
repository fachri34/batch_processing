import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col, when
from dotenv import load_dotenv

dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster(spark_host)
        .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
    ))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

file_table_pairs = [
    ("/data/olist/olist_customers_dataset.csv", "public.olist_customers_dataset"),
    ("/data/olist/olist_geolocation_dataset.csv", "public.olist_geolocation_dataset"),
    ("/data/olist/olist_order_items_dataset.csv", "public.olist_order_items_dataset"),
    ("/data/olist/olist_order_payments_dataset.csv", "public.olist_order_payments_dataset"),
    ("/data/olist/olist_order_reviews_dataset.csv", "public.olist_order_reviews_dataset"),
    ("/data/olist/olist_orders_dataset.csv", "public.olist_orders_dataset"),
    ("/data/olist/olist_products_dataset.csv", "public.olist_products_dataset"),
    ("/data/olist/olist_sellers_dataset.csv", "public.olist_sellers_dataset")
]

for file_path, table_name in file_table_pairs:

    df = spark.read.csv(file_path, header=True)
    
    (
        df
        .write
        .mode("overwrite")
        .jdbc(
            jdbc_url,
            table_name,
            properties=jdbc_properties
        )
    )
    print(f"Data from {file_path} has been written to {table_name}.")

result_df_customer = spark.read.jdbc(
    jdbc_url,
    'public.olist_sellers_dataset',
    properties=jdbc_properties
)

result_df_customer.show(5)
