"""
Spark job to archive old data from MongoDB to HDFS
Also updates Hive metastore automatically
"""

import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pymongo import MongoClient

def archive_mongodb_to_hdfs():
    """Archive old MongoDB data to HDFS and update Hive"""
    
    print("=" * 80)
    print("📦 STARTING MONGODB → HDFS ARCHIVING")
    print("=" * 80)
    
    # Create Spark session with MongoDB connector AND Hive support
    print("🔧 Creating Spark session...")
    spark = SparkSession.builder \
        .appName("MongoDBArchiver") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark session created")
    
    try:
        # Connect to MongoDB
        print("🔧 Connecting to MongoDB...")
        client = MongoClient("mongodb://mongodb:27017/")
        db = client['ecommerce']
        collection = db.orders
        
        # Get current size
        stats = db.command("dbStats")
        size_mb = stats.get("dataSize", 0) / (1024 * 1024)
        total_orders = collection.count_documents({})
        
        print(f"📊 MongoDB Status:")
        print(f"   - Size: {size_mb:.2f} MB")
        print(f"   - Orders: {total_orders:,}")
        print(f"   - Threshold: 300 MB")
        
        if size_mb < 300:
            print(f"✅ Size below threshold. No archiving needed.")
            client.close()
            spark.stop()
            return
        
        print(f"⚠️ Size exceeds threshold! Initiating archival...")
        
        # Calculate threshold date (keep last 2 hours, archive older)
        threshold_date = datetime.now() - timedelta(minutes=5)
        threshold_iso = threshold_date.isoformat()
        
        print(f"🔍 Archiving orders older than: {threshold_iso}")
        
        # Find old records
        old_records_query = {"order.timestamp": {"$lt": threshold_iso}}
        old_records_count = collection.count_documents(old_records_query)
        
        if old_records_count == 0:
            print("✅ No old records to archive.")
            client.close()
            spark.stop()
            return
        
        print(f"📦 Found {old_records_count:,} records to archive")
        
        # Read old records from MongoDB using Spark
        print("📥 Reading data from MongoDB via Spark...")
        df = spark.read.format("mongo") \
            .option("uri", "mongodb://mongodb:27017/ecommerce.orders") \
            .option("pipeline", json.dumps([{"$match": old_records_query}])) \
            .load()
        
        record_count = df.count()
        
        if record_count == 0:
            print("⚠️ No records found in Spark DataFrame")
            client.close()
            spark.stop()
            return
        
        print(f"✅ Loaded {record_count:,} records in Spark DataFrame")
        
        # Create archive ID
        archive_id = f"archive_{int(datetime.now().timestamp())}"
        hdfs_path = f"hdfs://namenode:9000/archives/ecommerce/{archive_id}"
        
        print(f"💾 Writing to HDFS: {hdfs_path}")
        
        # Write to HDFS as Parquet
        df.write.mode("overwrite").parquet(hdfs_path)
        
        print(f"✅ Successfully wrote {record_count:,} records to HDFS")
        
        # Create metadata
        metadata = {
            "archive_id": archive_id,
            "timestamp": datetime.now().isoformat(),
            "source": "mongodb",
            "database": "ecommerce",
            "collection": "orders",
            "threshold_date": threshold_iso,
            "record_count": record_count,
            "storage_location": hdfs_path,
            "mongodb_size_before_mb": size_mb,
            "format": "parquet"
        }
        
        # Write metadata to HDFS
        metadata_path = f"hdfs://namenode:9000/archives/metadata/{archive_id}_metadata.json"
        metadata_json = json.dumps(metadata, indent=2)
        
        print(f"📝 Writing metadata to: {metadata_path}")
        spark.sparkContext.parallelize([metadata_json]).saveAsTextFile(metadata_path)
        
        print(f"✅ Metadata written successfully")
        
        # ========================================
        # UPDATE HIVE METASTORE 
        # ========================================
        print("\n🗄️ Updating Hive Metastore...")
        
        try:
            # Create database if not exists
            spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_warehouse")
            spark.sql("USE ecommerce_warehouse")
            
            # Create archive metadata table
            spark.sql("""
                CREATE TABLE IF NOT EXISTS archive_metadata (
                    archive_id STRING,
                    archive_timestamp STRING,
                    source STRING,
                    record_count BIGINT,
                    storage_location STRING,
                    mongodb_size_before_mb DOUBLE,
                    created_at TIMESTAMP
                )
                STORED AS PARQUET
                LOCATION 'hdfs://namenode:9000/user/hive/warehouse/archive_metadata'
            """)
            
            # Insert metadata into Hive
            spark.sql(f"""
                INSERT INTO archive_metadata VALUES (
                    '{archive_id}',
                    '{metadata['timestamp']}',
                    '{metadata['source']}',
                    {metadata['record_count']},
                    '{metadata['storage_location']}',
                    {metadata['mongodb_size_before_mb']},
                    current_timestamp()
                )
            """)
            
            print("✅ Archive metadata added to Hive")
            
            # Create external table for this specific archive
            table_name = f"archived_orders_{archive_id}"
            
            spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
                    _id STRING,
                    order_col STRUCT<order_id:STRING, timestamp:STRING, total_amount:DOUBLE, quantity:INT, payment_method:STRING, status:STRING>,
                    customer STRUCT<customer_id:STRING, name:STRING, email:STRING, state:STRING>,
                    product STRUCT<product_id:STRING, name:STRING, category:STRING, price:DOUBLE>
                )
                STORED AS PARQUET
                LOCATION '{hdfs_path}'
            """)
            
            print(f"✅ Created Hive table: {table_name}")
            
            # Show Hive summary
            total_archives = spark.sql("SELECT COUNT(*) as count FROM archive_metadata").first()['count']
            print(f"📊 Total archives in Hive: {total_archives}")
            
        except Exception as e:
            print(f"⚠️ Could not update Hive metastore: {e}")
        
        # ========================================
        # DELETE FROM MONGODB
        # ========================================
        print(f"\n🗑️ Deleting {record_count:,} archived records from MongoDB...")
        result = collection.delete_many(old_records_query)
        deleted_count = result.deleted_count
        
        print(f"✅ Deleted {deleted_count:,} records from MongoDB")
        
        # Verify new size
        new_stats = db.command("dbStats")
        new_size_mb = new_stats['dataSize'] / (1024 * 1024)
        new_count = collection.count_documents({})
        saved_space = size_mb - new_size_mb
        
        print(f"\n📊 Archiving Complete:")
        print(f"   - Records Archived: {record_count:,}")
        print(f"   - HDFS Location: {hdfs_path}")
        print(f"   - MongoDB Size: {size_mb:.2f} MB → {new_size_mb:.2f} MB")
        print(f"   - Space Saved: {saved_space:.2f} MB")
        print(f"   - Remaining Orders: {new_count:,}")
        
        client.close()
        spark.stop()
        
        print("=" * 80)
        print("🎉 ARCHIVING COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Error in archiving process: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        raise

if __name__ == "__main__":
    archive_mongodb_to_hdfs()