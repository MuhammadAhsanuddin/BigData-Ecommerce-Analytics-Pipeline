"""
Spark Analytics DAG - Triggers Spark job to compute analytics and cache to Redis
Runs every 1 minute
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ecommerce',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'spark_analytics_pipeline',
    default_args=default_args,
    description='Run Spark analytics and cache results to Redis',
    schedule_interval=timedelta(minutes=1),  # Every 1 minute
    catchup=False,
    max_active_runs=1,  # Only one instance at a time
    tags=['analytics', 'spark', 'redis'],
)

# Spark job configuration
spark_conf = {
    "spark.driver.host": "airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.ui.port": "4040",
    "spark.pyspark.python": "python3.9",  # ADD THIS
    "spark.pyspark.driver.python": "python3.9",  # ADD THIS
}

# Submit Spark job
run_analytics = SparkSubmitOperator(
    task_id='run_spark_analytics',
    conn_id='spark_default',
    application='/opt/airflow/dags/analytics_job.py',
    conf=spark_conf,
    verbose=True,
    dag=dag,
)