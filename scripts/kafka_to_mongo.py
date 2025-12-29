from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import os

# Kafka setup
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=os.getenv('KAFKA_BROKER', 'kafka:9092'),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='mongo-consumer'
)

# MongoDB setup
mongo_client = MongoClient('mongodb://mongodb:27017/')
db = mongo_client['ecommerce']

print("✅ Consuming messages from Kafka and inserting into MongoDB (normalized)...")

for message in consumer:
    try:
        data = message.value
        
        # ========================================
        # NORMALIZED STORAGE (SEPARATE COLLECTIONS)
        # ========================================
        
        # 1. Insert/Update Customer (upsert to avoid duplicates)
        db.customers.update_one(
            {'customer_id': data['customer']['customer_id']},
            {'$set': data['customer']},
            upsert=True
        )
        
        # 2. Insert/Update Product (upsert to avoid duplicates)
        db.products.update_one(
            {'product_id': data['product']['product_id']},
            {'$set': data['product']},
            upsert=True
        )
        
        # 3. Insert Order (with foreign key references)
        db.orders.insert_one({
            'order_id': data['order']['order_id'],
            'customer_id': data['order']['customer_id'],  # Foreign key
            'product_id': data['order']['product_id'],    # Foreign key
            'timestamp': data['order']['timestamp'],
            'quantity': data['order']['quantity'],
            'unit_price': data['order']['unit_price'],
            'total_amount': data['order']['total_amount'],
            'shipping_cost': data['order']['shipping_cost'],
            'tax_amount': data['order']['tax_amount'],
            'order_status': data['order']['order_status']
        })
        
        # 4. Insert/Update Payment
        db.payments.update_one(
            {'payment_id': data['payment']['payment_id']},
            {'$set': data['payment']},
            upsert=True
        )
        
        print(f"✅ Inserted order: {data['order']['order_id']} | " +
              f"Customer: {data['customer']['customer_id']} | " +
              f"Product: {data['product']['product_id']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")