from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from pathlib import Path

default_args = {
    "owner": "dibimbing",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id=Path(__file__).stem,
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Test for spark submit postgresql etl",
    start_date=days_ago(1),
)

extract_task = SparkSubmitOperator(
    application="/spark-scripts/spark_etl_olist.py",
    conn_id="spark_main",
    task_id="spark_submit_task",
    jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",
    dag=spark_dag,
)

read_task = SparkSubmitOperator(
    application="/spark-scripts/spark_read_olist.py",
    conn_id="spark_main",
    task_id="spark_read_task",
    jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",
    dag=spark_dag,
)

extract_task >> read_task
