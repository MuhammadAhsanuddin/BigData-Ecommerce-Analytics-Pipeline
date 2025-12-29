"""
Data Quality Monitoring DAG
Monitors data quality metrics and alerts on anomalies
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import redis

default_args = {
    'owner': 'ecommerce',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_data_quality(**context):
    """Check data quality metrics"""
    
    print("=" * 80)
    print("🔍 DATA QUALITY CHECK")
    print("=" * 80)
    
    # Connect to MongoDB
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['ecommerce']
    
    # Connect to Redis
    r = redis.Redis(host='redis', port=6379, decode_responses=True)
    
    issues = []
    
    # Check 1: Duplicate orders
    print("\n📊 Check 1: Duplicate Orders")
    pipeline = [
        {"$group": {"_id": "$order.order_id", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    duplicates = list(db.orders.aggregate(pipeline))
    duplicate_count = len(duplicates)
    
    if duplicate_count > 0:
        issues.append(f"⚠️ Found {duplicate_count} duplicate order IDs")
        print(f"   ⚠️ {duplicate_count} duplicates found")
    else:
        print("   ✅ No duplicates")
    
    # Check 2: Null values
    print("\n📊 Check 2: Missing Data")
    null_orders = db.orders.count_documents({"order.order_id": None})
    null_customers = db.orders.count_documents({"customer.customer_id": None})
    
    if null_orders > 0:
        issues.append(f"⚠️ {null_orders} orders with null order_id")
    if null_customers > 0:
        issues.append(f"⚠️ {null_customers} orders with null customer_id")
    
    print(f"   - Null order IDs: {null_orders}")
    print(f"   - Null customer IDs: {null_customers}")
    
    # Check 3: Invalid amounts
    print("\n📊 Check 3: Invalid Amounts")
    invalid_amounts = db.orders.count_documents({"order.total_amount": {"$lte": 0}})
    
    if invalid_amounts > 0:
        issues.append(f"⚠️ {invalid_amounts} orders with invalid amounts")
        print(f"   ⚠️ {invalid_amounts} invalid amounts")
    else:
        print("   ✅ All amounts valid")
    
    # Check 4: Data freshness
    print("\n📊 Check 4: Data Freshness")
    latest_order = db.orders.find_one(sort=[("order.timestamp", -1)])
    
    if latest_order:
        latest_time = datetime.fromisoformat(latest_order['order']['timestamp'])
        age_minutes = (datetime.now() - latest_time).total_seconds() / 60
        
        print(f"   - Latest order: {age_minutes:.1f} minutes ago")
        
        if age_minutes > 5:
            issues.append(f"⚠️ No new data in {age_minutes:.1f} minutes")
    
    # Check 5: Volume anomalies
    print("\n📊 Check 5: Volume Anomalies")
    total_orders = db.orders.count_documents({})
    print(f"   - Total orders: {total_orders:,}")
    
    # Store quality metrics in Redis
    quality_metrics = {
        'duplicate_count': duplicate_count,
        'null_orders': null_orders,
        'null_customers': null_customers,
        'invalid_amounts': invalid_amounts,
        'total_orders': total_orders,
        'issues_count': len(issues),
        'last_check': datetime.now().isoformat()
    }
    
    r.set('data_quality:metrics', str(quality_metrics))
    
    # Summary
    print("\n" + "=" * 80)
    if issues:
        print(f"⚠️ DATA QUALITY ISSUES FOUND: {len(issues)}")
        for issue in issues:
            print(f"   {issue}")
    else:
        print("✅ ALL DATA QUALITY CHECKS PASSED!")
    print("=" * 80)
    
    client.close()
    
    # Push to XCom
    context['ti'].xcom_push(key='quality_issues', value=len(issues))
    context['ti'].xcom_push(key='quality_metrics', value=quality_metrics)
    
    return len(issues)

dag = DAG(
    'data_quality_monitoring',
    default_args=default_args,
    description='Monitor data quality metrics',
    schedule_interval=timedelta(minutes=10),  # Every 10 minutes
    catchup=False,
    tags=['monitoring', 'quality'],
)

quality_check = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)