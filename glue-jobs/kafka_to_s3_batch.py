#!/usr/bin/env python3

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    LongType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# Parse command line arguments
args = {}
argv = sys.argv[1:]
i = 0
while i < len(argv):
    if argv[i].startswith("--"):
        key = argv[i][2:]
        if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
            args[key] = argv[i + 1]
            i += 2
        else:
            args[key] = "true"
            i += 1
    else:
        i += 1


# MinIO/S3 Configuration
s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123")
warehouse_path = args.get("iceberg_warehouse", "s3a://warehouse/")

print("Starting Kafka to S3 batch job...")
print(f"S3 Endpoint: {s3_endpoint}")
print(f"Warehouse: {warehouse_path}")


# Initialize Spark session
spark = (
    SparkSession.builder.appName(args.get("JOB_NAME", "kafka-to-s3-batch"))
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Kafka options
kafka_options = {
    "kafka.bootstrap.servers": args.get("kafka_bootstrap_servers", "kafka:9092"),
    "subscribe": args.get("kafka_topic", "dbserver1.ecommerce.orders"),
    "startingOffsets": "earliest",
    "kafka.security.protocol": "PLAINTEXT",
}

print(f"Reading from Kafka: {kafka_options}")


# Read batch from Kafka
kafka_df = spark.read.format("kafka").options(**kafka_options).load()

print(f"Total Kafka messages: {kafka_df.count()}")


if kafka_df.count() > 0:
    # Schema for parsing order data
    order_schema = StructType(
        [
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", LongType(), True),
            StructField("status", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("shipping_address", StringType(), True),
        ]
    )
    
    # Parse JSON values from Kafka
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), order_schema).alias("data"),
        col("timestamp"),
        col("topic"),
    )

    # Select columns for output
    result_df = parsed_df.select(
        col("data.order_id"),
        col("data.customer_id"),
        col("data.order_date"),
        col("data.status"),
        col("data.total_amount"),
        col("data.shipping_address"),
        col("timestamp").alias("kafka_timestamp"),
        col("topic"),
    )

    print(f"Parsed {result_df.count()} records")

    # Write to Parquet in S3
    output_path = f"{warehouse_path}orders_parquet/"
    print(f"Writing to: {output_path}")

    result_df.coalesce(1).write.mode("append").parquet(output_path)

    print(f"Successfully wrote data to {output_path}")

    # List output
    print("\nOutput files:")
    result_df.printSchema()
else:
    print("No messages found in Kafka topic")


spark.stop()
print("Job completed!")

