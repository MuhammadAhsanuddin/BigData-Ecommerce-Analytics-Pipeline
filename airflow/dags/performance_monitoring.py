"""
Performance Monitoring DAG
Tracks system performance and resource usage
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
import redis
import subprocess

default_args = {
    'owner': 'ecommerce',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def monitor_performance(**context):
    """Monitor system performance"""
    
    print("=" * 80)
    print("📈 PERFORMANCE MONITORING")
    print("=" * 80)
    
    metrics = {}
    
    # MongoDB metrics
    print("\n💾 MongoDB Metrics:")
    client = MongoClient('mongodb://mongodb:27017/')
    db = client['ecommerce']
    
    stats = db.command("dbStats")
    metrics['mongodb'] = {
        'size_mb': stats['dataSize'] / (1024 * 1024),
        'index_size_mb': stats['indexSize'] / (1024 * 1024),
        'collections': stats['collections'],
        'objects': stats['objects']
    }
    
    print(f"   - DB Size: {metrics['mongodb']['size_mb']:.2f} MB")
    print(f"   - Index Size: {metrics['mongodb']['index_size_mb']:.2f} MB")
    print(f"   - Total Documents: {metrics['mongodb']['objects']:,}")
    
    # Redis metrics
    print("\n⚡ Redis Metrics:")
    r = redis.Redis(host='redis', port=6379, decode_responses=True)
    
    info = r.info()
    metrics['redis'] = {
        'used_memory_mb': info['used_memory'] / (1024 * 1024),
        'total_keys': r.dbsize(),
        'connected_clients': info['connected_clients']
    }
    
    print(f"   - Memory Used: {metrics['redis']['used_memory_mb']:.2f} MB")
    print(f"   - Total Keys: {metrics['redis']['total_keys']}")
    print(f"   - Clients: {metrics['redis']['connected_clients']}")
    
    # HDFS metrics
    print("\n📦 HDFS Metrics:")
    try:
        result = subprocess.run(
            ['hdfs', 'dfs', '-du', '-h', '/archives/ecommerce/'],
            capture_output=True,
            text=True
        )
        
        total_size = 0
        archive_count = 0
        
        for line in result.stdout.split('\n'):
            if 'archive_' in line:
                archive_count += 1
                parts = line.split()
                if len(parts) >= 2:
                    size_str = parts[0]
                    # Simple size parsing (you can make this more robust)
                    try:
                        if 'M' in size_str:
                            total_size += float(size_str.replace('M', ''))
                        elif 'G' in size_str:
                            total_size += float(size_str.replace('G', '')) * 1024
                    except:
                        pass
        
        metrics['hdfs'] = {
            'total_archives': archive_count,
            'total_size_mb': total_size
        }
        
        print(f"   - Total Archives: {archive_count}")
        print(f"   - Total Size: {total_size:.2f} MB")
        
    except Exception as e:
        print(f"   ⚠️ Could not fetch HDFS metrics: {e}")
        metrics['hdfs'] = {'error': str(e)}
    
    # Store metrics in Redis
    metrics['timestamp'] = datetime.now().isoformat()
    r.set('performance:metrics', str(metrics))
    
    print("\n" + "=" * 80)
    print("✅ PERFORMANCE MONITORING COMPLETE")
    print("=" * 80)
    
    client.close()
    
    context['ti'].xcom_push(key='performance_metrics', value=metrics)
    
    return metrics

dag = DAG(
    'performance_monitoring',
    default_args=default_args,
    description='Monitor system performance',
    schedule_interval=timedelta(minutes=15),  # Every 15 minutes
    catchup=False,
    tags=['monitoring', 'performance'],
)

perf_check = PythonOperator(
    task_id='monitor_performance',
    python_callable=monitor_performance,
    dag=dag,
)