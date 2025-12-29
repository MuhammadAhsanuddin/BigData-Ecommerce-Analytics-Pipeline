"""
Spark Analytics Job - Reads from MongoDB, Computes Analytics, Caches to Redis
This runs ON THE SPARK CLUSTER, triggered by Airflow
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg, count, col
from pyspark.sql.types import *
from datetime import datetime
import json
import redis
from pymongo import MongoClient

def run_analytics():
    """Main analytics function"""
    
    print("=" * 80)
    print("📊 STARTING SPARK ANALYTICS JOB")
    print("=" * 80)
    
    # Create Spark session
    print("🔧 Creating Spark session...")
    spark = SparkSession.builder \
        .appName("EcommerceAnalytics") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session created")
    
    # Connect to services
    print("🔧 Connecting to MongoDB and Redis...")
    mongo_client = MongoClient('mongodb://mongodb:27017/')
    db = mongo_client['ecommerce']
    r = redis.Redis(host='redis', port=6379, decode_responses=True)
    
    # Test connections
    r.ping()
    print("✅ Connected to Redis")
    print("✅ Connected to MongoDB")
    
    # Load data from MongoDB
    print("📥 Loading orders from MongoDB...")
    orders = list(db.orders.find())
    total_orders = len(orders)
    
    print(f"✅ Loaded {total_orders:,} orders from MongoDB")
    
    if total_orders == 0:
        print("⚠️ No data in MongoDB. Exiting.")
        spark.stop()
        mongo_client.close()
        return
    
    # Flatten nested MongoDB documents
    print("🔄 Flattening data structure...")
    flat_data = []
    for doc in orders:
        flat_data.append({
            'order_id': doc['order']['order_id'],
            'timestamp': doc['order']['timestamp'],
            'total_amount': doc['order']['total_amount'],
            'quantity': doc['order']['quantity'],
            'state': doc['customer']['state'],
            'product_name': doc['product']['name'],
            'category': doc['product']['category'],
        })
    
    # Define schema
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("state", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
    ])
    
    # Create Spark DataFrame
    print("🔄 Creating Spark DataFrame...")
    df = spark.createDataFrame(flat_data, schema)
    df.createOrReplaceTempView("orders")
    print(f"✅ DataFrame created with {df.count():,} rows")
    
    # ========================================
    # ANALYTICS QUERY 1: Overall Summary
    # ========================================
    print("\n📊 Query 1: Computing Overall Summary...")
    total_revenue = float(df.agg(_sum('total_amount')).collect()[0][0])
    avg_order_value = float(df.agg(avg('total_amount')).collect()[0][0])
    
    summary = {
        'total_orders': total_orders,
        'total_revenue': round(total_revenue, 2),
        'avg_order_value': round(avg_order_value, 2),
        'last_updated': datetime.now().isoformat()
    }
    
    r.set('analytics:summary', json.dumps(summary))
    print(f"   ✅ Total Orders: {total_orders:,}")
    print(f"   ✅ Total Revenue: ${total_revenue:,.2f}")
    print(f"   ✅ Avg Order Value: ${avg_order_value:.2f}")
    print(f"   ✅ Cached to Redis: analytics:summary")
    
    # ========================================
    # ANALYTICS QUERY 2: Top 10 Products
    # ========================================
    print("\n📊 Query 2: Computing Top 10 Products...")
    top_products_df = spark.sql("""
        SELECT 
            product_name,
            category,
            SUM(quantity) as total_quantity,
            SUM(total_amount) as total_revenue,
            COUNT(*) as order_count
        FROM orders
        GROUP BY product_name, category
        ORDER BY total_quantity DESC
        LIMIT 10
    """)
    
    top_products = [row.asDict() for row in top_products_df.collect()]
    r.set('analytics:top_products', json.dumps(top_products))
    print(f"   ✅ Cached {len(top_products)} top products to Redis")
    
    # ========================================
    # ANALYTICS QUERY 3: Revenue by Minute
    # ========================================
    print("\n📊 Query 3: Computing Revenue Per Minute...")
    revenue_by_min_df = spark.sql("""
        SELECT 
            substring(timestamp, 1, 16) as minute,
            SUM(total_amount) as total_revenue,
            COUNT(*) as order_count
        FROM orders
        GROUP BY substring(timestamp, 1, 16)
        ORDER BY minute DESC
        LIMIT 60
    """)
    
    revenue_by_min = [row.asDict() for row in revenue_by_min_df.collect()]
    r.set('analytics:revenue_by_minute', json.dumps(revenue_by_min))
    print(f"   ✅ Cached {len(revenue_by_min)} minute data points to Redis")
    
    # ========================================
    # ANALYTICS QUERY 4: Orders by State
    # ========================================
    print("\n📊 Query 4: Computing Orders by State...")
    by_state_df = spark.sql("""
        SELECT 
            state,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value
        FROM orders
        GROUP BY state
        ORDER BY order_count DESC
        LIMIT 20
    """)
    
    by_state = [row.asDict() for row in by_state_df.collect()]
    r.set('analytics:orders_by_state', json.dumps(by_state))
    print(f"   ✅ Cached {len(by_state)} state statistics to Redis")
    
    # ========================================
    # ANALYTICS QUERY 5: Category Performance
    # ========================================
    print("\n📊 Query 5: Computing Category Performance...")
    by_category_df = spark.sql("""
        SELECT 
            category,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            SUM(quantity) as total_quantity
        FROM orders
        GROUP BY category
        ORDER BY total_revenue DESC
    """)
    
    by_category = [row.asDict() for row in by_category_df.collect()]
    r.set('analytics:category_performance', json.dumps(by_category))
    print(f"   ✅ Cached {len(by_category)} category statistics to Redis")
    
    # Cleanup
    print("\n🧹 Cleaning up...")
    spark.stop()
    mongo_client.close()
    
    print("=" * 80)
    print("🎉 SPARK ANALYTICS COMPLETED SUCCESSFULLY!")
    print(f"📊 Processed {total_orders:,} orders")
    print(f"💰 Total Revenue: ${total_revenue:,.2f}")
    print(f"📦 Cached 5 analytics datasets to Redis")
    print("=" * 80)

if __name__ == "__main__":
    run_analytics()