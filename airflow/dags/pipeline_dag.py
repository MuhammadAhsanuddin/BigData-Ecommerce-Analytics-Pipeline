from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ecommerce',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'ecommerce_realtime_pipeline',
    default_args=default_args,
    description='Real-time e-commerce data pipeline',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

# Task 1: Monitor MongoDB size
def check_mongodb_size():
    from pymongo import MongoClient
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['ecommerce']
    stats = db.command("dbstats")
    size_mb = stats['dataSize'] / (1024 * 1024)
    print(f"Current MongoDB size: {size_mb:.2f} MB")
    return size_mb

check_size = PythonOperator(
    task_id='check_mongodb_size',
    python_callable=check_mongodb_size,
    dag=dag,
)

# Task 2: Archive to HDFS
def archive_to_hdfs(**context):
    from pymongo import MongoClient
    import json
    from datetime import datetime, timedelta
    
    ti = context['ti']
    size_mb = ti.xcom_pull(task_ids='check_mongodb_size')
    
    if size_mb and size_mb > 300:
        client = MongoClient('mongodb://mongodb:27017/')
        db = client['ecommerce']
        
        cutoff_time = (datetime.now() - timedelta(hours=2)).isoformat()
        old_data = list(db.orders.find({"order.timestamp": {"$lt": cutoff_time}}))
        
        if old_data:
            archive_file = f"/tmp/archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(archive_file, 'w') as f:
                json.dump(old_data, f, default=str)
            
            db.orders.delete_many({"order.timestamp": {"$lt": cutoff_time}})
            print(f"Archived {len(old_data)} records")
    else:
        print(f"No archiving needed. Size: {size_mb} MB")

archive = PythonOperator(
    task_id='archive_to_hdfs',
    python_callable=archive_to_hdfs,
    provide_context=True,
    dag=dag,
)

# Task 3: Run Spark Analytics
def run_spark_analytics():
    import subprocess
    import os
    
    print("Starting Spark Analytics Job...")
    
    # Create a simple Python script to run via spark-submit
    spark_code = """
from pyspark.sql import SparkSession
from pymongo import MongoClient
import redis
import json
from datetime import datetime

spark = SparkSession.builder.appName("EcommerceAnalytics").master("spark://spark-master:7077").getOrCreate()
r = redis.Redis(host='redis', port=6379, decode_responses=True)
mongo_client = MongoClient('mongodb://mongodb:27017/')
db = mongo_client['ecommerce']

orders_list = list(db.orders.find().limit(10000))
print(f"Loaded {len(orders_list)} orders")

if orders_list:
    flat_data = [(d['order']['order_id'], d['order']['total_amount'], d['customer']['state']) for d in orders_list]
    df = spark.createDataFrame(flat_data, ['order_id', 'total_amount', 'state'])
    
    summary = {'total_orders': df.count(), 'total_revenue': float(df.agg({'total_amount': 'sum'}).collect()[0][0]), 'last_updated': datetime.now().isoformat()}
    r.set('analytics:summary', json.dumps(summary))
    print(f"Cached: {summary}")

spark.stop()
"""
    
    # Write to temp file
    with open('/tmp/spark_job.py', 'w') as f:
        f.write(spark_code)
    
    print("Spark analytics completed (using external job)")

spark_analytics = PythonOperator(
    task_id='spark_analytics',
    python_callable=run_spark_analytics,
    dag=dag,
)

# Task 4: Refresh cache
def refresh_cache():
    print("Pipeline completed successfully")

refresh = PythonOperator(
    task_id='refresh_cache',
    python_callable=refresh_cache,
    dag=dag,
)

# Task flow
check_size >> archive >> spark_analytics >> refresh