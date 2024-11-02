import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp,col,when
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

df_order = spark.read.jdbc(
    jdbc_url,
    'public.olist_order_items_dataset',
    properties=jdbc_properties
)

df_product = spark.read.jdbc(
    jdbc_url,
    'public.olist_products_dataset',
    properties = jdbc_properties
)

df_payment = spark.read.jdbc(
    jdbc_url,
    'public.olist_order_payments_dataset',
    properties = jdbc_properties
)

count_payment = df_payment.groupBy('payment_type').agg(F.count('payment_type').alias('count_payment_type')).orderBy('count_payment_type', ascending=False)

count_payment.show()

