import streamlit as st
import pymongo
import redis
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import json
import os

# Page configuration
st.set_page_config(
    page_title="E-Commerce Real-Time Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# MongoDB connection
@st.cache_resource
def get_mongo_client():
    mongo_host = os.getenv('MONGO_HOST', 'mongodb')
    client = pymongo.MongoClient(f'mongodb://{mongo_host}:27017/')
    return client

# Redis connection
@st.cache_resource
def get_redis_client():
    redis_host = os.getenv('REDIS_HOST', 'redis')
    return redis.Redis(host=redis_host, port=6379, decode_responses=True)

mongo_client = get_mongo_client()
redis_client = get_redis_client()
db = mongo_client['ecommerce']

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .big-font {
        font-size: 48px !important;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("🛒 E-Commerce Real-Time Analytics Dashboard")
st.markdown("### Live Data Updates Every 60 Seconds")

# Placeholder for auto-refresh
placeholder = st.empty()

def fetch_data_from_cache(key, query_func):
    """Fetch from Redis cache or compute"""
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)
    else:
        data = query_func()
        redis_client.setex(key, 60, json.dumps(data))  # Cache for 60 seconds
        return data

def get_total_revenue():
    """Calculate total revenue from last hour"""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    pipeline = [
        {"$match": {"order.timestamp": {"$gte": one_hour_ago.isoformat()}}},
        {"$group": {"_id": None, "total": {"$sum": "$order.total_amount"}}}
    ]
    result = list(db.orders.aggregate(pipeline))
    return round(result[0]['total'], 2) if result else 0

def get_orders_count():
    """Count orders in last hour"""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    return db.orders.count_documents({"order.timestamp": {"$gte": one_hour_ago.isoformat()}})

def get_avg_order_value():
    """Calculate average order value"""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    pipeline = [
        {"$match": {"order.timestamp": {"$gte": one_hour_ago.isoformat()}}},
        {"$group": {"_id": None, "avg": {"$avg": "$order.total_amount"}}}
    ]
    result = list(db.orders.aggregate(pipeline))
    return round(result[0]['avg'], 2) if result else 0

def get_payment_success_rate():
    """Calculate payment success rate"""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    total = db.orders.count_documents({"payment.timestamp": {"$gte": one_hour_ago.isoformat()}})
    success = db.orders.count_documents({
        "payment.timestamp": {"$gte": one_hour_ago.isoformat()},
        "payment.status": "success"
    })
    return round((success / total * 100), 2) if total > 0 else 0

def get_revenue_by_minute():
    """Revenue trend per minute (last hour)"""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    pipeline = [
        {"$match": {"order.timestamp": {"$gte": one_hour_ago.isoformat()}}},
        {"$group": {
            "_id": {"$substr": ["$order.timestamp", 0, 16]},  # Group by minute
            "revenue": {"$sum": "$order.total_amount"}
        }},
        {"$sort": {"_id": 1}}
    ]
    results = list(db.orders.aggregate(pipeline))
    return [{"time": r["_id"], "revenue": r["revenue"]} for r in results]

def get_top_products():
    """Top 5 products by quantity sold"""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    pipeline = [
        {"$match": {"order.timestamp": {"$gte": one_hour_ago.isoformat()}}},
        {"$group": {
            "_id": "$product.name",
            "quantity": {"$sum": "$order.quantity"},
            "revenue": {"$sum": "$order.total_amount"}
        }},
        {"$sort": {"quantity": -1}},
        {"$limit": 5}
    ]
    results = list(db.orders.aggregate(pipeline))
    return [{"product": r["_id"], "quantity": r["quantity"], "revenue": r["revenue"]} for r in results]

def get_orders_by_state():
    """Orders distribution by state"""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    pipeline = [
        {"$match": {"order.timestamp": {"$gte": one_hour_ago.isoformat()}}},
        {"$group": {
            "_id": "$customer.state",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = list(db.orders.aggregate(pipeline))
    return [{"state": r["_id"], "orders": r["count"]} for r in results]

# Main loop for auto-refresh
while True:
    with placeholder.container():
        
        # KPI Row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            revenue = get_total_revenue()
            st.metric("💰 Total Revenue (1h)", f"${revenue:,.2f}", delta="Live")
        
        with col2:
            orders = get_orders_count()
            st.metric("📦 Total Orders (1h)", f"{orders:,}", delta="Live")
        
        with col3:
            avg_order = get_avg_order_value()
            st.metric("💵 Avg Order Value", f"${avg_order:.2f}", delta="Live")
        
        with col4:
            success_rate = get_payment_success_rate()
            st.metric("✅ Payment Success Rate", f"{success_rate}%", delta="Live")
        
        st.markdown("---")
        
        # Charts Row 1
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Revenue Per Minute (Last Hour)")
            revenue_data = get_revenue_by_minute()
            if revenue_data:
                df_revenue = pd.DataFrame(revenue_data)
                fig = px.line(df_revenue, x='time', y='revenue', 
                             title="Revenue Trend",
                             labels={'revenue': 'Revenue ($)', 'time': 'Time'})
                fig.update_traces(line_color='#667eea', line_width=3)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available yet")
        
        with col2:
            st.subheader("🏆 Top 5 Products by Sales")
            products_data = get_top_products()
            if products_data:
                df_products = pd.DataFrame(products_data)
                fig = px.bar(df_products, x='quantity', y='product', 
                            orientation='h',
                            title="Best Selling Products",
                            labels={'quantity': 'Units Sold', 'product': 'Product'},
                            color='revenue',
                            color_continuous_scale='Viridis')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available yet")
        
        # Charts Row 2
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🗺️ Orders by State (Top 10)")
            state_data = get_orders_by_state()
            if state_data:
                df_states = pd.DataFrame(state_data)
                fig = px.pie(df_states, values='orders', names='state',
                            title="Geographic Distribution")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available yet")
        
        with col2:
            st.subheader("📊 Live Statistics")
            stats_data = {
                "Metric": ["Total Customers", "Total Products", "Avg Processing Time", "Data Size (MB)"],
                "Value": [
                    db.orders.distinct("customer.customer_id").__len__(),
                    db.orders.distinct("product.product_id").__len__(),
                    "0.8s",  # Placeholder
                    round(db.command("dbstats")["dataSize"] / (1024*1024), 2)
                ]
            }
            st.dataframe(pd.DataFrame(stats_data), use_container_width=True, hide_index=True)
        
        # Footer
        st.markdown("---")
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.caption(f"🔄 Last updated: {current_time} | Refreshing in 60 seconds...")
    
    # Wait 60 seconds before refresh
    time.sleep(60)
    st.rerun()