FROM apache/spark:3.5.1

USER root

# Install Python packages
RUN pip install --no-cache-dir pymongo==4.6.1 redis==5.0.1

# Switch back to spark user
USER spark

WORKDIR /opt/spark/work-dir