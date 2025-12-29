FROM apache/spark:3.5.1

USER root

# Install Python 3.9
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y python3.9 python3.9-distutils python3-pip && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1 && \
    apt-get clean

# Install Python packages
RUN pip3 install --no-cache-dir pymongo redis

USER 185

ENV PYSPARK_PYTHON=python3.9
ENV PYSPARK_DRIVER_PYTHON=python3.9