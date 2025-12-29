FROM apache/airflow:2.9.3-python3.9

USER root

# 1. Install OpenJDK-11 (Required for Spark) and procps
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    apt-get clean

# 2. Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# 3. Install Apache Spark binaries (Client side)
# UPDATED: We use 3.5.1 to match the Airflow constraints
RUN curl -o spark-3.5.1-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz

# 4. Set SPARK_HOME and add to PATH
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 5. Copy initialization script (as root)
COPY init-airflow.sh /init-airflow.sh
RUN chmod +x /init-airflow.sh && chown airflow:root /init-airflow.sh

USER airflow

# 6. Install Python dependencies
# UPDATED: We removed '==3.5.0'. The constraint file will automatically install 3.5.1
RUN pip install apache-airflow-providers-apache-spark pyspark pymongo redis \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.9.txt"