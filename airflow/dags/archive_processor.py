"""
Spark job to archive old data from MongoDB to HDFS
Archives data older than 2 hours when MongoDB > 20 MB (for demo purposes)
"""

import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient

def archive_mongodb_to_hdfs():
    """Archive old MongoDB data to HDFS"""
    
    print("=" * 80)
    print("📦 STARTING MONGODB → HDFS ARCHIVING")
    print("=" * 80)
    
    # Create Spark session with MongoDB connector
    print("🔧 Creating Spark session...")
    spark = SparkSession.builder \
        .appName("MongoDBArchiver") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
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
        print(f"   - Threshold: 50 MB")
        
        if size_mb < 20:
            print(f"✅ Size below threshold. No archiving needed.")
            client.close()
            spark.stop()
            return
        
        print(f"⚠️ Size exceeds threshold! Initiating archival...")
        
        # Calculate threshold date (keep last 2 hours, archive older)
        threshold_date = datetime.now() - timedelta(hours=2)
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
        
        # Delete archived records from MongoDB
        print(f"🗑️ Deleting {record_count:,} archived records from MongoDB...")
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