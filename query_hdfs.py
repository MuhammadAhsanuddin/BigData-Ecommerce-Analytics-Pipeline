from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QueryHDFS") \
    .getOrCreate()

# Read from HDFS
df = spark.read.parquet("hdfs://namenode:9000/archives/ecommerce/archive_1766991842")

# Count
print("\n" + "="*80)
print("TOTAL ARCHIVED ORDERS FROM HDFS")
print("="*80)
print(f"Total: {df.count():,} orders")

# Analytics by category
print("\n" + "="*80)
print("REVENUE BY CATEGORY (FROM HDFS COLD STORAGE)")
print("="*80)
result = df.groupBy("product.category") \
    .agg({"order.total_amount": "sum", "*": "count"}) \
    .orderBy("sum(order.total_amount)", ascending=False)

result.show()

spark.stop()