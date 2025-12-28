import os
import json
import time
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
from kafka import KafkaProducer
import random

fake = Faker()
Faker.seed(42)
np.random.seed(42)

# Statistical Parameters
ORDERS_PER_MINUTE = 50  # Poisson lambda
PEAK_HOURS = [10, 11, 12, 18, 19, 20]  # Peak shopping times
CUSTOMER_POOL_SIZE = 10000
PRODUCT_POOL_SIZE = 500

# Initialize Kafka Producer with retry logic
def get_kafka_producer():
    max_retries = 30
    retry_count = 0
    while retry_count < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(0, 10, 1)
            )
            print("✅ Successfully connected to Kafka!")
            return producer
        except Exception as e:
            retry_count += 1
            print(f"⏳ Waiting for Kafka... (attempt {retry_count}/{max_retries})")
            time.sleep(5)
    raise Exception("❌ Failed to connect to Kafka after 30 attempts")

producer = get_kafka_producer()

# Pre-generate customer pool (Zipf distribution - 20% customers make 80% orders)
print("Generating customer pool...")
customers = []
for i in range(CUSTOMER_POOL_SIZE):
    customers.append({
        'customer_id': f'CUST_{i:06d}',
        'name': fake.name(),
        'email': fake.email(),
        'city': fake.city(),
        'state': fake.state(),
        'country': 'USA',
        'registration_date': fake.date_between(start_date='-2y', end_date='today').isoformat(),
        'segment': np.random.choice(['Premium', 'Regular', 'Budget'], p=[0.2, 0.5, 0.3])
    })

# Pre-generate product pool
print("Generating product pool...")
categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Sports', 'Toys']
products = []
for i in range(PRODUCT_POOL_SIZE):
    category = np.random.choice(categories)
    cost_price = np.random.lognormal(mean=3.5, sigma=1.0)  # Log-normal distribution
    products.append({
        'product_id': f'PROD_{i:05d}',
        'name': fake.catch_phrase(),
        'category': category,
        'subcategory': f'{category}_Sub_{np.random.randint(1, 5)}',
        'brand': fake.company(),
        'cost_price': round(cost_price, 2),
        'list_price': round(cost_price * np.random.uniform(1.3, 2.5), 2)
    })

print("🚀 Starting data generation...")

order_id_counter = 0

def generate_order():
    global order_id_counter
    
    # Time-based ordering patterns (Beta distribution for peak hours)
    current_hour = datetime.now().hour
    if current_hour in PEAK_HOURS:
        order_rate_multiplier = np.random.beta(2, 5) + 1.5  # Higher during peak
    else:
        order_rate_multiplier = np.random.beta(5, 2) * 0.5  # Lower off-peak
    
    # Zipf distribution for customer selection (power law)
    customer_idx = int(np.random.zipf(1.5) - 1) % CUSTOMER_POOL_SIZE
    customer = customers[customer_idx]
    
    # Product selection (also follows Zipf - popular products sell more)
    product_idx = int(np.random.zipf(1.3) - 1) % PRODUCT_POOL_SIZE
    product = products[product_idx]
    
    # Quantity follows geometric distribution (most buy 1-2 items)
    quantity = np.random.geometric(p=0.6)
    
    # Pricing with discounts (Normal distribution around list price)
    discount_factor = np.random.normal(0.95, 0.1)
    discount_factor = max(0.7, min(1.0, discount_factor))  # Clamp between 70%-100%
    unit_price = round(product['list_price'] * discount_factor, 2)
    
    total_amount = round(unit_price * quantity, 2)
    
    # Shipping cost (exponential distribution)
    shipping_cost = round(np.random.exponential(scale=5.0) + 2.99, 2)
    
    # Tax (normal around 8%)
    tax_rate = np.random.normal(0.08, 0.01)
    tax_amount = round(total_amount * tax_rate, 2)
    
    # Payment processing time (gamma distribution)
    processing_time = round(np.random.gamma(2, 0.5), 2)
    
    order_id_counter += 1
    
    order = {
        'order_id': f'ORD_{order_id_counter:08d}',
        'customer_id': customer['customer_id'],
        'product_id': product['product_id'],
        'timestamp': datetime.now().isoformat(),
        'quantity': quantity,
        'unit_price': unit_price,
        'total_amount': total_amount,
        'shipping_cost': shipping_cost,
        'tax_amount': tax_amount,
        'order_status': np.random.choice(['completed', 'pending', 'cancelled'], p=[0.85, 0.10, 0.05])
    }
    
    payment = {
        'payment_id': f'PAY_{order_id_counter:08d}',
        'order_id': order['order_id'],
        'payment_method': np.random.choice(['credit_card', 'debit_card', 'paypal', 'crypto'], p=[0.5, 0.3, 0.15, 0.05]),
        'status': np.random.choice(['success', 'failed'], p=[0.95, 0.05]),
        'processing_time': processing_time,
        'timestamp': datetime.now().isoformat()
    }
    
    # Combine for streaming
    event = {
        'order': order,
        'customer': customer,
        'product': product,
        'payment': payment
    }
    
    return event

# Main loop
while True:
    try:
        # Poisson distribution for number of orders per interval
        orders_this_interval = np.random.poisson(ORDERS_PER_MINUTE / 12)  # Per 5 seconds
        
        for _ in range(orders_this_interval):
            event = generate_order()
            producer.send('orders', value=event)
            print(f"📦 Sent order: {event['order']['order_id']} | Amount: ${event['order']['total_amount']}")
        
        time.sleep(5)  # Generate every 5 seconds
        
    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(5)