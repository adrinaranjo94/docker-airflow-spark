FROM apache/airflow:2.5.0-python3.9

# Install Spark
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    curl -O https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar xvf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2 /opt/spark && \
    rm spark-3.1.2-bin-hadoop3.2.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Install additional Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
