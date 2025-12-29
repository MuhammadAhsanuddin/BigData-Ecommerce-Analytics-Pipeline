# 🛒 Real-Time Big Data Analytics Pipeline for E-Commerce

A fully dockerized, production-style **real-time Big Data Analytics (BDA) pipeline** for the e-commerce domain.  
This project demonstrates **streaming ingestion, distributed analytics, hot/cold storage, automated archiving, caching, and live dashboards** using industry-standard big data technologies.

---

## 📌 Business Domain & Problem

### **Domain: E-Commerce**

Modern e-commerce platforms generate **high-velocity transactional data** from:
- Order placements  
- Customer activity  
- Product purchases  
- Payments and checkout flows  

This data is **continuous, time-sensitive, and operationally critical**.

### **Problem Statement**

Traditional batch-based analytics systems provide insights **hours or days late**, which can lead to:
- Missed revenue opportunities  
- Delayed detection of payment failures  
- Inability to react to demand spikes (e.g., flash sales)  
- Poor operational visibility  

**Core problem:**  
> Lack of real-time visibility into key e-commerce metrics.

### **Project Goal**

Design and implement a **real-time analytics pipeline** that:
- Continuously ingests streaming data  
- Computes KPIs with minimal latency  
- Supports both **real-time dashboards** and **historical analysis**  
- Manages data growth using automated hot/cold storage  

---

## 🏗️ System Architecture Overview
![architecture](https://github.com/user-attachments/assets/6128a95b-bfb2-454b-bc69-08069d432abb)


The pipeline follows a **Lambda-style architecture** with a clear separation between:

- **Hot path:** real-time ingestion, analytics, and dashboards  
- **Cold path:** long-term archival and historical analysis  

All components are **containerized using Docker** and **orchestrated via Apache Airflow**.

---

## 🔄 End-to-End Data Flow

1. **Data Generation**  
   - Python-based generator simulates realistic e-commerce transactions using statistical models  
   - Events are produced in JSON format  

2. **Streaming Ingestion**  
   - Apache Kafka buffers high-velocity order streams  
   - Ensures producer–consumer decoupling  

3. **Hot Storage**  
   - MongoDB stores recent transactional data for low-latency access  

4. **Analytics Processing**  
   - Apache Spark reads data from MongoDB  
   - Executes OLAP-style analytics using Spark SQL  

5. **Caching Layer**  
   - Redis caches precomputed analytics results  
   - Enables sub-millisecond dashboard access  

6. **Dashboard Layer**  
   - BI dashboard reads analytics directly from Redis  
   - Auto-refreshes every minute  

7. **Archiving & Lifecycle Management**  
   - Airflow monitors MongoDB size  
   - When **300 MB threshold** is exceeded:
     - Older data is archived to HDFS (Parquet)
     - Hive Metastore catalogs archived datasets
     - Archived records are deleted from MongoDB  

---

## 🧰 Technology Stack

| Layer | Technology | Purpose |
|-----|-----------|--------|
| Data Generation | Python | Simulates e-commerce transactions |
| Streaming | Apache Kafka + Zookeeper | High-throughput message ingestion |
| Hot Storage | MongoDB | Low-latency operational data |
| Processing | Apache Spark (PySpark) | Distributed analytics |
| Caching | Redis | Fast KPI access |
| Cold Storage | Hadoop HDFS | Long-term historical storage |
| Metadata | Hive Metastore | Archive metadata management |
| Orchestration | Apache Airflow | Scheduling & workflow control |
| Deployment | Docker & Docker Compose | Containerized environment |

---

## 🧠 Data Model & Schema

### **Schema Design**

The system uses a **fact–dimension (star) schema** optimized for BI and OLAP workloads.

### **Fact Table: `orders_fact`**

Captures transactional metrics:
- order_id  
- customer_id  
- product_id  
- order_timestamp  
- quantity  
- unit_price  
- total_amount  
- shipping_cost  
- tax_amount  
- order_status  

**Key KPIs:**
- Total revenue  
- Total orders  
- Average order value  
- Unique Customers  
- Payment Success

### **Dimension Tables**
- `customers_dim` – customer attributes and segments  
- `products_dim` – product categories and pricing  
- `payments_dim` – payment method and performance  
- Time attributes derived dynamically from timestamps  

---

## 📊 Analytics Performed

Spark executes OLAP-style queries every minute, including:
- Revenue per minute  
- Top-selling products  
- Orders by state  
- Payment success and failure rates  
- Customer segment analysis  
- Overall business summary  

All results are cached in Redis for fast retrieval.

---

## 📦 Data Volume Management & Archiving

### **Hot Data Policy**
- MongoDB stores recent, high-value data
- Optimized for low-latency analytics

### **Archiving Policy**
- Threshold: **300 MB**
- Triggered automatically via Airflow
- Older records are:
  - Written to HDFS in **Parquet format**
  - Registered in Hive Metastore
  - Deleted from MongoDB after successful archival

### **Benefits**
- Prevents performance degradation  
- Enables historical analytics  
- Mirrors real-world production pipelines  

---

## 🔁 Airflow Orchestration (DAGs)

| DAG Name | Purpose |
|--------|--------|
| `spark_analytics_pipeline` | Triggers Spark analytics every minute |
| `mongodb_archiving_pipeline` | Archives data to HDFS and Hive |
| `data_generation_monitoring` | Kafka & MongoDB health checks |
| `data_quality_monitoring` | Data correctness validation |
| `performance_monitoring` | System-level metrics monitoring |

Each DAG follows a **single-responsibility design** for reliability and maintainability.

---

## 📈 Dashboard
<img width="1600" height="757" alt="image" src="https://github.com/user-attachments/assets/6e529aac-4f08-4118-bbc5-146407c8afa3" />
<img width="1600" height="692" alt="image" src="https://github.com/user-attachments/assets/8577721a-1022-40b6-8c27-25e0f2dab5dc" />
<img width="1600" height="735" alt="image" src="https://github.com/user-attachments/assets/880f5ccb-f2be-491d-a144-de76137c3bbb" />
<img width="1600" height="582" alt="image" src="https://github.com/user-attachments/assets/4efc1cfb-a20e-41bf-9249-8478cff0fef6" />
<img width="1600" height="758" alt="image" src="https://github.com/user-attachments/assets/9749e7c5-2caf-4b78-a082-e8c8e0cd0de0" />

---

## ▶️ How to Run the Project

### **Prerequisites**
- Docker & Docker Compose  
- Minimum **4 GB RAM** allocated to Docker  

### **Setup**

```bash
git clone <your-repository-url>
cd <project-directory>
docker-compose up -d --build
