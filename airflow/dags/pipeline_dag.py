from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

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
    schedule_interval=timedelta(minutes=1),  # Run every minute
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
    
    # Trigger archive if > 300MB
    if size_mb > 300:
        return 'archive_to_hdfs'
    return 'skip_archive'

check_size = PythonOperator(
    task_id='check_mongodb_size',
    python_callable=check_mongodb_size,
    dag=dag,
)

# Task 2: Archive to HDFS (if needed)
def archive_to_hdfs():
    from pymongo import MongoClient
    import json
    from datetime import datetime
    
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['ecommerce']
    
    # Export oldest data
    cutoff_time = (datetime.now() - timedelta(hours=2)).isoformat()
    old_data = list(db.orders.find({"order.timestamp": {"$lt": cutoff_time}}))
    
    if old_data:
        # Save to HDFS (simplified - use hdfs Python library in production)
        archive_file = f"/tmp/archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(archive_file, 'w') as f:
            json.dump(old_data, f, default=str)
        
        # Delete archived data from MongoDB
        db.orders.delete_many({"order.timestamp": {"$lt": cutoff_time}})
        print(f"Archived {len(old_data)} records")

archive = PythonOperator(
    task_id='archive_to_hdfs',
    python_callable=archive_to_hdfs,
    dag=dag,
)

# Task 3: Refresh cache
def refresh_cache():
    import redis
    r = redis.Redis(host='redis', port=6379)
    r.flushdb()  # Clear cache to force dashboard refresh
    print("Cache refreshed")

refresh = PythonOperator(
    task_id='refresh_cache',
    python_callable=refresh_cache,
    dag=dag,
)

# Task flow
check_size >> archive >> refresh