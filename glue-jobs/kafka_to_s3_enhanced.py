#!/usr/bin/env python3

import os
import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, max as spark_max
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType,
)


# Parse command line arguments
args = {}
argv = sys.argv[1:]
i = 0
while i < len(argv):
    if argv[i].startswith("--"):
        arg = argv[i][2:]
        if "=" in arg:
            key, value = arg.split("=", 1)
            args[key] = value
            i += 1
        else:
            key = arg
            if i + 1 < len(argv) and not argv[i + 1].startswith("--"):
                args[key] = argv[i + 1]
                i += 2
            else:
                args[key] = "true"
                i += 1
    else:
        i += 1


# Configuration
s3_endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
s3_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
s3_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "admin123")
warehouse_path = args.get("iceberg_warehouse", "s3a://warehouse/")
table_name = args.get("table_name", "orders")
offset_file = f"/tmp/cdc_offsets/{table_name}.json"


print("=" * 60)
print("CDC SYNC JOB - Kafka to S3")
print("=" * 60)
print(f"Table: {table_name}")
print(f"Warehouse: {warehouse_path}")
print(f"Offset File: {offset_file}")
print("=" * 60)


# Initialize Spark session
spark = (
    SparkSession.builder.appName(args.get("JOB_NAME", f"kafka-to-s3-{table_name}"))
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ===== Read last saved offset =====
last_offset = -1  # -1 means no previous offset (read from beginning)
if os.path.exists(offset_file):
    try:
        with open(offset_file, 'r') as f:
            data = json.load(f)
            last_offset = data.get('offset', -1)
            print(f"Last processed offset: {last_offset}")
    except Exception as e:
        print(f"Could not read offset file: {e}")
        last_offset = -1


# Kafka configuration - USE THE SAVED OFFSET
kafka_topic = args.get("kafka_topic", f"dbserver1.ecommerce.{table_name}")
kafka_options = {
    "kafka.bootstrap.servers": args.get("kafka_bootstrap_servers", "kafka:9092"),
    "subscribe": kafka_topic,
    "kafka.security.protocol": "PLAINTEXT",
}

# Set starting offset based on last saved position
if last_offset >= 0:
    # Read from specific offset onwards (only new messages)
    kafka_options["startingOffsets"] = json.dumps({
        kafka_topic: {0: last_offset + 1}  # +1 to skip already processed
    })
    print(f"Reading NEW messages only (from offset {last_offset + 1})")
else:
    # First run - read all existing messages
    kafka_options["startingOffsets"] = "earliest"
    print("First run - reading ALL messages from beginning")

print(f"Subscribing to Kafka topic: {kafka_topic}")


# Define schema based on table type
def get_table_schema(table):
    """Return the appropriate schema for each table"""
    schemas = {
        'orders': StructType([
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", LongType(), True),
            StructField("status", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("shipping_address", StringType(), True),
        ]),
        'customers': StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("phone", StringType(), True),
        ]),
        'products': StructType([
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", StringType(), True),
            StructField("stock_quantity", IntegerType(), True),
        ]),
        'order_items': StructType([
            StructField("order_item_id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", StringType(), True),
            StructField("subtotal", StringType(), True),
        ]),
    }
    return schemas.get(table, schemas['orders'])


# Read from Kafka
try:
    kafka_df = spark.read.format("kafka").options(**kafka_options).load()
    total_messages = kafka_df.count()
    print(f"New messages found: {total_messages}")
except Exception as e:
    print(f"Error reading from Kafka: {e}")
    spark.stop()
    sys.exit(1)


if total_messages > 0:
    # Parse JSON values
    table_schema = get_table_schema(table_name)
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("key"),
        from_json(col("value").cast("string"), table_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("topic"),
        col("offset"),
    )

    # Select columns based on table type
    if table_name == 'orders':
        result_df = parsed_df.select(
            col("data.order_id"), col("data.customer_id"), col("data.order_date"),
            col("data.status"), col("data.total_amount"), col("data.shipping_address"),
            col("kafka_timestamp"), col("topic"), col("offset").alias("kafka_offset"),
        )
    elif table_name == 'customers':
        result_df = parsed_df.select(
            col("data.customer_id"), col("data.email"), col("data.first_name"),
            col("data.last_name"), col("data.phone"),
            col("kafka_timestamp"), col("topic"), col("offset").alias("kafka_offset"),
        )
    elif table_name == 'products':
        result_df = parsed_df.select(
            col("data.product_id"), col("data.product_name"), col("data.category"),
            col("data.price"), col("data.stock_quantity"),
            col("kafka_timestamp"), col("topic"), col("offset").alias("kafka_offset"),
        )
    else:  # order_items
        result_df = parsed_df.select(
            col("data.order_item_id"), col("data.order_id"), col("data.product_id"),
            col("data.quantity"), col("data.unit_price"), col("data.subtotal"),
            col("kafka_timestamp"), col("topic"), col("offset").alias("kafka_offset"),
        )

    new_records = result_df.count()
    print(f"Parsed {new_records} new records")

    # Get max offset for tracking
    max_offset = result_df.agg(spark_max("kafka_offset")).collect()[0][0]
    print(f"Max new offset: {max_offset}")

    # Write to Parquet in S3
    output_path = f"{warehouse_path}{table_name}_parquet/"
    print(f"Writing to: {output_path}")

    result_df.coalesce(1).write.mode("append").parquet(output_path)
    print(f"Successfully wrote {new_records} records to {output_path}")

    # ===== SAVE OFFSET FOR NEXT RUN =====
    os.makedirs("/tmp/cdc_offsets", exist_ok=True)
    with open(offset_file, 'w') as f:
        json.dump({
            'offset': max_offset,
            'timestamp': str(datetime.now()),
            'records_processed': new_records
        }, f)
    print(f"Saved offset {max_offset} to {offset_file}")
    print(f"Next run will only read messages from offset {max_offset + 1}")

    # Show sample data
    print("\nSample new data:")
    result_df.show(3, truncate=False)
else:
    print("No NEW messages in Kafka - database has no changes")
    print("If you INSERT/UPDATE data in PostgreSQL, Debezium will capture it")
    print("Next run will automatically pick up new changes")


spark.stop()
print("\n" + "=" * 60)
print("CDC SYNC JOB COMPLETED")
print("=" * 60)

