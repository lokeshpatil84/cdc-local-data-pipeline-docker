FROM python:3.9-slim

ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# System deps
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    curl \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2 for S3/MinIO operations
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip && \
    unzip -q /tmp/awscliv2.zip -d /tmp && \
    /tmp/aws/install && \
    rm -rf /tmp/awscliv2.zip /tmp/aws

# Spark install - with progress bar and retries for slow mirrors
RUN wget \
  --progress=bar:force \
  --timeout=60 \
  --tries=3 \
  https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
  tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
  mv spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
  rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# REQUIRED JARS (Kafka + S3 + Iceberg) - All baked into image
RUN wget -P $SPARK_HOME/jars/ \
 https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
 wget -P $SPARK_HOME/jars/ \
 https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
 wget -P $SPARK_HOME/jars/ \
 https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar && \
 wget -P $SPARK_HOME/jars/ \
 https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \
 wget -P $SPARK_HOME/jars/ \
 https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
 wget -P $SPARK_HOME/jars/ \
 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
 wget -P $SPARK_HOME/jars/ \
 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.610/aws-java-sdk-bundle-1.12.610.jar

# Python deps
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    boto3==1.34.0 \
    kafka-python==2.0.2 \
    amazon-ion==0.10.0 \
    pyiceberg==0.10.0 \
    s3fs==2024.2.0

# Create workspace directories
RUN mkdir -p /workspace /workspace/warehouse

WORKDIR /workspace

# Copy job scripts
COPY glue-jobs/ /workspace/glue-jobs/

CMD ["/bin/bash"]

