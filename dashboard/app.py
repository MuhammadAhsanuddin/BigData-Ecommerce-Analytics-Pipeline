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
    .cache-indicator {
        color: #00FF00;
        font-size: 12px;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("🛒 E-Commerce Real-Time Analytics Dashboard")
st.markdown("### Live Data Updates Every 10 Seconds")

def get_from_redis_or_mongo(redis_key, mongo_fallback_func):
    """
    Try to get data from Redis cache first (fast!).
    If not available, fall back to MongoDB query (slow).
    """
    try:
        cached_data = redis_client.get(redis_key)
        if cached_data:
            return json.loads(cached_data), True  # True = from cache
    except Exception as e:
        pass
    
    # Fallback to MongoDB
    return mongo_fallback_func(), False  # False = from MongoDB

def get_total_revenue():
    """Get revenue from Redis cache (updated by Spark)"""
    def mongo_fallback():
        pipeline = [
            {"$group": {"_id": None, "total": {"$sum": "$order.total_amount"}}}
        ]
        result = list(db.orders.aggregate(pipeline))
        return round(result[0]['total'], 2) if result else 0
    
    summary_data, from_cache = get_from_redis_or_mongo('analytics:summary', mongo_fallback)
    
    if from_cache and isinstance(summary_data, dict):
        return summary_data.get('total_revenue', 0), from_cache
    
    return summary_data, from_cache

def get_orders_count():
    """Get order count from Redis cache"""
    def mongo_fallback():
        return db.orders.count_documents({})
    
    summary_data, from_cache = get_from_redis_or_mongo('analytics:summary', mongo_fallback)
    
    if from_cache and isinstance(summary_data, dict):
        return summary_data.get('total_orders', 0), from_cache
    
    return summary_data, from_cache

def get_avg_order_value():
    """Get average order value from Redis cache"""
    def mongo_fallback():
        pipeline = [
            {"$group": {"_id": None, "avg": {"$avg": "$order.total_amount"}}}
        ]
        result = list(db.orders.aggregate(pipeline))
        return round(result[0]['avg'], 2) if result else 0
    
    summary_data, from_cache = get_from_redis_or_mongo('analytics:summary', mongo_fallback)
    
    if from_cache and isinstance(summary_data, dict):
        return summary_data.get('avg_order_value', 0), from_cache
    
    return summary_data, from_cache

def get_payment_success_rate():
    """Calculate payment success rate from MongoDB or Redis"""
    # Try to get from summary first
    try:
        summary_str = redis_client.get('analytics:summary')
        if summary_str:
            summary_data = json.loads(summary_str)
            # If we add payment_success_rate to Spark analytics, use it
            if 'payment_success_rate' in summary_data:
                return summary_data['payment_success_rate'], True
    except:
        pass
    
    # Fallback: Calculate from payments collection
    try:
        total = db.payments.count_documents({})
        success = db.payments.count_documents({"status": "success"})
        rate = round((success / total * 100), 2) if total > 0 else 0
        return rate, False
    except:
        return 0.0, False

def get_revenue_by_minute():
    """Get revenue trend from Redis cache"""
    def mongo_fallback():
        pipeline = [
            {"$group": {
                "_id": {"$substr": ["$order.timestamp", 0, 16]},
                "revenue": {"$sum": "$order.total_amount"}
            }},
            {"$sort": {"_id": -1}},
            {"$limit": 60}
        ]
        results = list(db.orders.aggregate(pipeline))
        return [{"minute": r["_id"], "revenue": r["revenue"]} for r in results]
    
    data, from_cache = get_from_redis_or_mongo('analytics:revenue_by_minute', mongo_fallback)
    return data if data else [], from_cache

def get_top_products():
    """Get top products from Redis cache"""
    def mongo_fallback():
        pipeline = [
            {"$group": {
                "_id": "$product.name",
                "quantity": {"$sum": "$order.quantity"},
                "revenue": {"$sum": "$order.total_amount"}
            }},
            {"$sort": {"quantity": -1}},
            {"$limit": 10}
        ]
        results = list(db.orders.aggregate(pipeline))
        return [{"product_name": r["_id"], "total_quantity": r["quantity"], "total_revenue": r["revenue"]} for r in results]
    
    data, from_cache = get_from_redis_or_mongo('analytics:top_products', mongo_fallback)
    return data if data else [], from_cache

def get_orders_by_state():
    """Get orders by state from Redis cache"""
    def mongo_fallback():
        pipeline = [
            {"$group": {
                "_id": "$customer.state",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        results = list(db.orders.aggregate(pipeline))
        return [{"state": r["_id"], "order_count": r["count"]} for r in results]
    
    data, from_cache = get_from_redis_or_mongo('analytics:orders_by_state', mongo_fallback)
    return data if data else [], from_cache

def format_cache_indicator(from_cache):
    """Show where data came from"""
    if from_cache:
        return "⚡ Redis Cache"
    return "🐢 MongoDB (Direct)"

# Main content
# KPI Row
col1, col2, col3, col4 = st.columns(4)

with col1:
    revenue, cached = get_total_revenue()
    st.metric("💰 Total Revenue (All Time)", f"${revenue:,.2f}", delta=format_cache_indicator(cached))

with col2:
    orders, cached = get_orders_count()
    st.metric("📦 Total Orders (All Time)", f"{orders:,}", delta=format_cache_indicator(cached))

with col3:
    avg_order, cached = get_avg_order_value()
    st.metric("💵 Avg Order Value", f"${avg_order:.2f}", delta=format_cache_indicator(cached))

with col4:
    success_rate, cached = get_payment_success_rate()
    st.metric("✅ Payment Success Rate", f"{success_rate}%", delta=format_cache_indicator(cached))

st.markdown("---")

# Charts Row 1
col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Revenue Per Minute (Last 60 Minutes)")
    revenue_data, cached = get_revenue_by_minute()
    
    if revenue_data and len(revenue_data) > 0:
        df_revenue = pd.DataFrame(revenue_data)
        time_col = 'minute' if 'minute' in df_revenue.columns else 'time'
        revenue_col = 'total_revenue' if 'total_revenue' in df_revenue.columns else 'revenue'
        
        fig = px.line(df_revenue, x=time_col, y=revenue_col, 
                     title=f"Revenue Trend ({format_cache_indicator(cached)})",
                     labels={revenue_col: 'Revenue ($)', time_col: 'Time'})
        fig.update_traces(line_color='#667eea', line_width=3)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("⏳ Waiting for Spark analytics to populate revenue data...")

with col2:
    st.subheader("🏆 Top 10 Products by Sales")
    products_data, cached = get_top_products()
    
    if products_data and len(products_data) > 0:
        df_products = pd.DataFrame(products_data)
        product_col = 'product_name' if 'product_name' in df_products.columns else 'product'
        quantity_col = 'total_quantity' if 'total_quantity' in df_products.columns else 'quantity'
        revenue_col = 'total_revenue' if 'total_revenue' in df_products.columns else 'revenue'
        
        fig = px.bar(df_products, x=quantity_col, y=product_col, 
                    orientation='h',
                    title=f"Best Selling Products ({format_cache_indicator(cached)})",
                    labels={quantity_col: 'Units Sold', product_col: 'Product'},
                    color=revenue_col,
                    color_continuous_scale='Viridis')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("⏳ Waiting for Spark analytics to populate product data...")

# Charts Row 2
col1, col2 = st.columns(2)

with col1:
    st.subheader("🗺️ Orders by State (Top 10)")
    state_data, cached = get_orders_by_state()
    
    if state_data and len(state_data) > 0:
        df_states = pd.DataFrame(state_data)
        state_col = 'state' if 'state' in df_states.columns else 'state'
        orders_col = 'order_count' if 'order_count' in df_states.columns else 'orders'
        
        fig = px.pie(df_states, values=orders_col, names=state_col,
                    title=f"Geographic Distribution ({format_cache_indicator(cached)})")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("⏳ Waiting for Spark analytics to populate state data...")

with col2:
    st.subheader("📊 Live System Statistics")
    
    # Get unique customers/products from Redis (computed by Spark JOIN)
    try:
        summary_str = redis_client.get('analytics:summary')
        if summary_str:
            summary_data = json.loads(summary_str)
            unique_customers = summary_data.get('unique_customers', 0)
            unique_products = summary_data.get('unique_products', 0)
        else:
            unique_customers = 0
            unique_products = 0
    except Exception as e:
        st.error(f"Redis error: {e}")
        unique_customers = 0
        unique_products = 0
    
    # Get Redis info
    try:
        redis_keys = redis_client.keys('analytics:*')
        redis_status = f"✅ {len(redis_keys)} cached analytics"
    except:
        redis_status = "❌ Redis unavailable"
    
    # Get MongoDB stats
    try:
        db_stats = db.command("dbstats")
        mongo_size_mb = round(db_stats["dataSize"] / (1024*1024), 2)
        mongo_status = f"💾 {mongo_size_mb} MB"
    except:
        mongo_status = "❌ MongoDB unavailable"
    

    stats_data = {
        "Metric": [
            "Unique Customers", 
            "Unique Products", 
            "MongoDB Size",
            "Redis Cache Status"
        ],
        "Value": [
            f"{unique_customers:,}",
            f"{unique_products:,}",
            mongo_status,
            redis_status
        ]
    }
    st.dataframe(pd.DataFrame(stats_data), use_container_width=True, hide_index=True, height=200)
    
    # Show last Spark update time
    try:
        summary_str = redis_client.get('analytics:summary')
        if summary_str:
            data = json.loads(summary_str)
            last_update = data.get('last_updated', 'Unknown')
            st.caption(f"🔄 Last Spark Analytics Update: {last_update}")
            st.caption(f"⚡ Data from Spark SQL JOINS")
        else:
            st.caption("⏳ Waiting for first Spark analytics run...")
    except:
        st.caption("⚠️ Unable to get Spark update status")

# Footer
st.markdown("---")
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

col1, col2, col3 = st.columns(3)
with col1:
    st.caption(f"🕐 Dashboard Updated: {current_time}")
with col2:
    st.caption("🔄 Auto-refresh: Every 10 seconds")
with col3:
    st.caption(f"⚙️ Rerun count: {st.session_state.get('rerun_count', 0)}")  # ← ADD THIS

# Increment rerun counter
if 'rerun_count' not in st.session_state:
    st.session_state.rerun_count = 0
st.session_state.rerun_count += 1

# Auto-refresh mechanism
time.sleep(2)
st.rerun()