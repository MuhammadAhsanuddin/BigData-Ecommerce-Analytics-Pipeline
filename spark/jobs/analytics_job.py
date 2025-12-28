from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json
import redis
from pymongo import MongoClient

# Initialize Spark (without MongoDB connector)
spark = SparkSession.builder \
    .appName("EcommerceAnalytics") \
    .getOrCreate()

# Redis connection
r = redis.Redis(host='redis', port=6379, decode_responses=True)

# MongoDB connection using PyMongo
mongo_client = MongoClient('mongodb://mongodb:27017/')
db = mongo_client['ecommerce']

print("📊 Starting Spark Analytics Job...")

# Read from MongoDB using PyMongo and convert to Spark DataFrame
orders_list = list(db.orders.find())
print(f"✅ Loaded {len(orders_list)} orders from MongoDB")

if len(orders_list) == 0:
    print("⚠️ No data in MongoDB yet. Exiting.")
    spark.stop()
    exit(0)

# Define schema
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("shipping_cost", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("state", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("processing_time", DoubleType(), True)
])

# Flatten data
flat_data = []
for doc in orders_list:
    flat_data.append({
        'order_id': doc['order']['order_id'],
        'timestamp': doc['order']['timestamp'],
        'total_amount': doc['order']['total_amount'],
        'quantity': doc['order']['quantity'],
        'shipping_cost': doc['order']['shipping_cost'],
        'tax_amount': doc['order']['tax_amount'],
        'customer_id': doc['customer']['customer_id'],
        'state': doc['customer']['state'],
        'segment': doc['customer']['segment'],
        'product_id': doc['product']['product_id'],
        'product_name': doc['product']['name'],
        'category': doc['product']['category'],
        'payment_method': doc['payment']['payment_method'],
        'payment_status': doc['payment']['status'],
        'processing_time': doc['payment']['processing_time']
    })

# Create Spark DataFrame
orders_df = spark.createDataFrame(flat_data, schema)
orders_df.createOrReplaceTempView("orders")

print(f"✅ Created Spark DataFrame with {orders_df.count()} rows")

# OLAP Query 1: Revenue by minute (last hour)
revenue_by_minute = spark.sql("""
    SELECT 
        substring(timestamp, 1, 16) as minute,
        SUM(total_amount) as total_revenue,
        COUNT(*) as order_count
    FROM orders
    GROUP BY substring(timestamp, 1, 16)
    ORDER BY minute DESC
    LIMIT 60
""")

revenue_json = [row.asDict() for row in revenue_by_minute.collect()]
r.set('analytics:revenue_by_minute', json.dumps(revenue_json))
print(f"✅ Cached {len(revenue_json)} revenue data points")

# OLAP Query 2: Top products
top_products = spark.sql("""
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

top_products_json = [row.asDict() for row in top_products.collect()]
r.set('analytics:top_products', json.dumps(top_products_json))
print(f"✅ Cached top {len(top_products_json)} products")

# OLAP Query 3: Orders by state
orders_by_state = spark.sql("""
    SELECT 
        state,
        COUNT(*) as order_count,
        SUM(total_amount) as total_revenue
    FROM orders
    GROUP BY state
    ORDER BY order_count DESC
    LIMIT 20
""")

state_json = [row.asDict() for row in orders_by_state.collect()]
r.set('analytics:orders_by_state', json.dumps(state_json))
print(f"✅ Cached data for {len(state_json)} states")

# OLAP Query 4: Payment metrics
payment_metrics = spark.sql("""
    SELECT 
        payment_method,
        payment_status,
        COUNT(*) as count,
        AVG(processing_time) as avg_processing_time
    FROM orders
    GROUP BY payment_method, payment_status
""")

payment_json = [row.asDict() for row in payment_metrics.collect()]
r.set('analytics:payment_metrics', json.dumps(payment_json))
print(f"✅ Cached payment metrics")

# OLAP Query 5: Customer segment analysis
segment_analysis = spark.sql("""
    SELECT 
        segment,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(*) as total_orders,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value
    FROM orders
    GROUP BY segment
""")

segment_json = [row.asDict() for row in segment_analysis.collect()]
r.set('analytics:segment_analysis', json.dumps(segment_json))
print(f"✅ Cached segment analysis")

# Summary statistics
summary = {
    'total_orders': orders_df.count(),
    'total_revenue': orders_df.agg(sum('total_amount')).collect()[0][0],
    'avg_order_value': orders_df.agg(avg('total_amount')).collect()[0][0],
    'last_updated': datetime.now().isoformat()
}
r.set('analytics:summary', json.dumps(summary))
print(f"✅ Cached summary: {summary['total_orders']} orders, ${summary['total_revenue']:.2f} revenue")

print("🎉 Analytics job completed successfully!")

spark.stop()