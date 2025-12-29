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
    """Main analytics function with JOINS using PyMongo + Spark"""
    
    print("=" * 80)
    print("📊 STARTING SPARK ANALYTICS JOB WITH JOINS")
    print("=" * 80)
    
    # Create Spark session (NO MONGO CONNECTOR NEEDED)
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
    
    r.ping()
    print("✅ Connected to Redis")
    print("✅ Connected to MongoDB")
    
    # ========================================
    # LOAD DATA FROM 3 NORMALIZED COLLECTIONS
    # ========================================
    print("📥 Loading data from 3 MongoDB collections...")
    
    orders_list = list(db.orders.find())
    customers_list = list(db.customers.find())
    products_list = list(db.products.find())
    
    print(f"✅ Loaded {len(orders_list):,} orders")
    print(f"✅ Loaded {len(customers_list):,} customers")
    print(f"✅ Loaded {len(products_list):,} products")
    
    if len(orders_list) == 0:
        print("⚠️ No orders in MongoDB. Exiting.")
        spark.stop()
        mongo_client.close()
        return
    
    # ========================================
    # CONVERT TO DATAFRAMES FOR SPARK SQL JOINS
    # ========================================
    print("🔄 Creating Spark DataFrames...")
    
    # Orders DataFrame
    orders_data = [{
        'order_id': o['order_id'],
        'customer_id': o['customer_id'],
        'product_id': o['product_id'],
        'timestamp': o['timestamp'],
        'quantity': o['quantity'],
        'total_amount': o['total_amount'],
    } for o in orders_list]
    
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
    ])
    
    orders_df = spark.createDataFrame(orders_data, orders_schema)
    orders_df.createOrReplaceTempView("orders")
    
    # Customers DataFrame
    customers_data = [{
        'customer_id': c['customer_id'],
        'name': c.get('name', 'Unknown'),
        'state': c.get('state', 'Unknown'),
        'segment': c.get('segment', 'Regular'),
        'city': c.get('city', 'Unknown'),
    } for c in customers_list]
    
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("city", StringType(), True),
    ])
    
    customers_df = spark.createDataFrame(customers_data, customers_schema)
    customers_df.createOrReplaceTempView("customers")
    
    # Products DataFrame
    products_data = [{
        'product_id': p['product_id'],
        'name': p.get('name', 'Unknown'),
        'category': p.get('category', 'Unknown'),
        'brand': p.get('brand', 'Unknown'),
    } for p in products_list]
    
    products_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
    ])
    
    products_df = spark.createDataFrame(products_data, products_schema)
    products_df.createOrReplaceTempView("products")
    
    print("✅ Created 3 Spark DataFrames")
    
    # ========================================
    # PERFORM SPARK SQL JOIN (THE KEY PART!)
    # ========================================
    print("\n🔗 Performing SPARK SQL INNER JOIN across 3 tables...")
    
    joined_df = spark.sql("""
        SELECT 
            o.order_id,
            o.timestamp,
            o.total_amount,
            o.quantity,
            o.customer_id,
            c.name as customer_name,
            c.state as customer_state,
            c.segment as customer_segment,
            c.city as customer_city,
            o.product_id,
            p.name as product_name,
            p.category as product_category,
            p.brand as product_brand
        FROM orders o
        INNER JOIN customers c ON o.customer_id = c.customer_id
        INNER JOIN products p ON o.product_id = p.product_id
    """)
    
    total_joined = joined_df.count()
    print(f"✅ INNER JOIN completed! {total_joined:,} records")
    print(f"   📊 Joined: orders ⟕ customers ⟕ products")
    
    joined_df.createOrReplaceTempView("orders_joined")
    
    # ========================================
    # QUERY 1: Overall Summary
    # ========================================
    print("\n📊 Query 1: Overall Summary...")
    
    summary_df = spark.sql("""
        SELECT 
            COUNT(*) as total_orders,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products
        FROM orders_joined
    """)
    
    summary_row = summary_df.collect()[0]
    summary = {
        'total_orders': int(summary_row['total_orders']),
        'total_revenue': round(float(summary_row['total_revenue']), 2),
        'avg_order_value': round(float(summary_row['avg_order_value']), 2),
        'unique_customers': int(summary_row['unique_customers']),
        'unique_products': int(summary_row['unique_products']),
        'last_updated': datetime.now().isoformat()
    }
    
    r.set('analytics:summary', json.dumps(summary))
    print(f"   ✅ Total Orders: {summary['total_orders']:,}")
    print(f"   ✅ Total Revenue: ${summary['total_revenue']:,.2f}")
    
    # ========================================
    # QUERY 2: Revenue by State & Category (WHERE + GROUP BY + HAVING)
    # ========================================
    print("\n📊 Query 2: Revenue by State & Category (with WHERE, GROUP BY, HAVING)...")
    
    state_category_df = spark.sql("""
        SELECT 
            customer_state,
            product_category,
            COUNT(*) as order_count,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value
        FROM orders_joined
        WHERE customer_state != 'Unknown' 
          AND product_category != 'Unknown'
        GROUP BY customer_state, product_category
        HAVING SUM(total_amount) > 50
        ORDER BY total_revenue DESC
        LIMIT 30
    """)
    
    state_category = [row.asDict() for row in state_category_df.collect()]
    r.set('analytics:state_category', json.dumps(state_category))
    print(f"   ✅ Cached {len(state_category)} state-category combinations")
    
    # ========================================
    # QUERY 3: Top Products
    # ========================================
    print("\n📊 Query 3: Top 10 Products...")
    
    top_products_df = spark.sql("""
        SELECT 
            product_name,
            product_category,
            product_brand,
            SUM(quantity) as total_quantity,
            SUM(total_amount) as total_revenue,
            COUNT(*) as order_count
        FROM orders_joined
        GROUP BY product_name, product_category, product_brand
        ORDER BY total_quantity DESC
        LIMIT 10
    """)
    
    top_products = [row.asDict() for row in top_products_df.collect()]
    r.set('analytics:top_products', json.dumps(top_products))
    print(f"   ✅ Cached {len(top_products)} top products")
    
    # ========================================
    # QUERY 4: Orders by State
    # ========================================
    print("\n📊 Query 4: Orders by State...")
    
    by_state_df = spark.sql("""
        SELECT 
            customer_state as state,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value
        FROM orders_joined
        WHERE customer_state != 'Unknown'
        GROUP BY customer_state
        ORDER BY order_count DESC
        LIMIT 20
    """)
    
    by_state = [row.asDict() for row in by_state_df.collect()]
    r.set('analytics:orders_by_state', json.dumps(by_state))
    print(f"   ✅ Cached {len(by_state)} states")
    
    # ========================================
    # QUERY 5: Category Performance
    # ========================================
    print("\n📊 Query 5: Category Performance...")
    
    by_category_df = spark.sql("""
        SELECT 
            product_category as category,
            COUNT(*) as order_count,
            SUM(total_amount) as total_revenue,
            SUM(quantity) as total_quantity
        FROM orders_joined
        GROUP BY product_category
        ORDER BY total_revenue DESC
    """)
    
    by_category = [row.asDict() for row in by_category_df.collect()]
    r.set('analytics:category_performance', json.dumps(by_category))
    print(f"   ✅ Cached {len(by_category)} categories")
    
    # ========================================
    # QUERY 6: Revenue by Minute
    # ========================================
    print("\n📊 Query 6: Revenue Per Minute...")
    
    revenue_by_min_df = spark.sql("""
        SELECT 
            substring(timestamp, 1, 16) as minute,
            SUM(total_amount) as total_revenue,
            COUNT(*) as order_count
        FROM orders_joined
        GROUP BY substring(timestamp, 1, 16)
        ORDER BY minute DESC
        LIMIT 60
    """)
    
    revenue_by_min = [row.asDict() for row in revenue_by_min_df.collect()]
    r.set('analytics:revenue_by_minute', json.dumps(revenue_by_min))
    print(f"   ✅ Cached {len(revenue_by_min)} minutes")
    
    # Cleanup
    print("\n🧹 Cleaning up...")
    spark.stop()
    mongo_client.close()
    
    print("=" * 80)
    print("🎉 SPARK ANALYTICS WITH SQL JOINS COMPLETED!")
    print(f"📊 Processed {total_joined:,} joined records")
    print(f"💰 Total Revenue: ${summary['total_revenue']:,.2f}")
    print(f"🔗 Used INNER JOIN: orders ⟕ customers ⟕ products")
    print(f"📦 Cached 6 analytics datasets to Redis")
    print("=" * 80)

if __name__ == "__main__":
    run_analytics()