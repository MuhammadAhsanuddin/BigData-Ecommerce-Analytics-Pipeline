from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'ecommerce',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'data_generation_monitoring',
    default_args=default_args,
    description='Monitor data generation pipeline health',
    schedule_interval=timedelta(minutes=5),  # Every 5 minutes
    catchup=False,
    tags=['monitoring', 'data-generation'],
)

def check_kafka_health():
    """Verify Kafka is accessible and has the orders topic"""
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    
    try:
        consumer = KafkaConsumer(
            bootstrap_servers='kafka:9092',
            consumer_timeout_ms=3000
        )
        
        topics = consumer.topics()
        
        if 'orders' in topics:
            logging.info(f"✅ Kafka is healthy - 'orders' topic exists")
            logging.info(f"📊 Available topics: {topics}")
        else:
            logging.warning(f"⚠️ 'orders' topic not found. Topics: {topics}")
        
        consumer.close()
        return True
        
    except NoBrokersAvailable:
        logging.error("❌ Cannot connect to Kafka!")
        raise
    except Exception as e:
        logging.error(f"❌ Kafka check failed: {e}")
        raise

def check_data_flowing():
    """Verify new data is being generated and consumed"""
    from pymongo import MongoClient
    from datetime import datetime, timedelta
    import time
    
    try:
        client = MongoClient('mongodb://mongodb:27017/')
        db = client['ecommerce']
        
        # Get initial count
        initial_count = db.orders.count_documents({})
        logging.info(f"📊 Current MongoDB orders: {initial_count:,}")
        
        if initial_count == 0:
            logging.warning("⚠️ MongoDB is empty - data generator might not have started yet")
            client.close()
            return False
        
        # Wait 10 seconds
        logging.info("⏳ Waiting 10 seconds to check for new data...")
        time.sleep(10)
        
        # Get new count
        new_count = db.orders.count_documents({})
        new_orders = new_count - initial_count
        
        if new_orders > 0:
            logging.info(f"✅ Data is flowing! {new_orders} new orders in 10 seconds")
        else:
            logging.warning("⚠️ No new data detected! Data generator might be down!")
            
        # Check recent data (last 2 minutes)
        two_min_ago = (datetime.now() - timedelta(minutes=2)).isoformat()
        recent_count = db.orders.count_documents({"order.timestamp": {"$gte": two_min_ago}})
        
        logging.info(f"📈 Orders in last 2 minutes: {recent_count}")
        
        if recent_count < 5:
            logging.warning("⚠️ Low data generation rate!")
        
        client.close()
        return new_orders > 0
        
    except Exception as e:
        logging.error(f"❌ MongoDB check failed: {e}")
        raise

def check_mongodb_health():
    """Check MongoDB connection and size"""
    from pymongo import MongoClient
    
    try:
        client = MongoClient('mongodb://mongodb:27017/')
        db = client['ecommerce']
        
        # Get database stats
        stats = db.command("dbstats")
        size_mb = stats['dataSize'] / (1024 * 1024)
        count = db.orders.count_documents({})
        
        logging.info(f"📊 MongoDB Health:")
        logging.info(f"   - Total Orders: {count:,}")
        logging.info(f"   - Database Size: {size_mb:.2f} MB")
        
        if count > 0:
            avg_doc_size = (size_mb * 1024) / count
            logging.info(f"   - Avg Document Size: {avg_doc_size:.2f} KB")
        
        if size_mb > 300:
            logging.warning(f"⚠️ MongoDB size > 300MB - archiving should trigger soon!")
        elif size_mb > 250:
            logging.warning(f"⚠️ MongoDB size > 250MB - approaching archiving threshold")
        else:
            logging.info(f"✅ MongoDB size OK ({size_mb:.2f} MB)")
        
        client.close()
        return True
        
    except Exception as e:
        logging.error(f"❌ MongoDB health check failed: {e}")
        raise

def check_data_generator_container():
    """Check if data generator container is running"""
    import subprocess
    
    try:
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=data-generator', '--format', '{{.Status}}'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0 and 'Up' in result.stdout:
            logging.info(f"✅ Data generator container is running: {result.stdout.strip()}")
            return True
        else:
            logging.error(f"❌ Data generator container is not running!")
            return False
            
    except Exception as e:
        logging.warning(f"⚠️ Could not check container status: {e}")
        logging.info("This is normal when running Airflow without Docker socket access")
        return True  # Don't fail the DAG if we can't check container status

# Task 1: Check Kafka
check_kafka_task = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

# Task 2: Check data is flowing
check_flow_task = PythonOperator(
    task_id='check_data_flowing',
    python_callable=check_data_flowing,
    dag=dag,
)

# Task 3: Check MongoDB health
check_mongo_task = PythonOperator(
    task_id='check_mongodb_health',
    python_callable=check_mongodb_health,
    dag=dag,
)

# Task 4: Check data generator container (optional)
check_generator_task = PythonOperator(
    task_id='check_data_generator_container',
    python_callable=check_data_generator_container,
    dag=dag,
)

# Task 5: Log completion
log_task = BashOperator(
    task_id='log_completion',
    bash_command='echo "✅ Data generation monitoring completed at $(date)"',
    dag=dag,
)

# Define workflow
# Run checks in parallel, then log
[check_kafka_task, check_flow_task, check_mongo_task, check_generator_task] >> log_task