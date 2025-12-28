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
collection = db['orders']

print("Consuming messages from Kafka and inserting into MongoDB...")

for message in consumer:
    try:
        data = message.value
        collection.insert_one(data)
        print(f"Inserted order: {data['order']['order_id']}")
    except Exception as e:
        print(f"Error: {e}")