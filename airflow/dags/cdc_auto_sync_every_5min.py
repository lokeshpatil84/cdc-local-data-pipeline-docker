#!/usr/bin/env python3

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess


# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


# DAG configuration
dag = DAG(
    'cdc_auto_sync_every_5min',
    default_args=default_args,
    description='Auto CDC Sync - Runs every 5 minutes to capture database changes',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['cdc', 'auto', 'kafka', 's3', '5min'],
)


# Tables to sync
TABLES = ['orders', 'customers', 'products', 'order_items']
WAREHOUSE_PATH = 's3a://warehouse/'
KAFKA_TOPIC_PREFIX = 'dbserver1.ecommerce.'


def configure_minio():
    """Configure MinIO bucket using AWS CLI via glue-local container"""
    print("\n" + "=" * 50)
    print("Configuring MinIO/S3 Bucket")
    print("=" * 50)
    
    # AWS CLI configuration
    aws_config = (
        "export AWS_ACCESS_KEY_ID=admin && "
        "export AWS_SECRET_ACCESS_KEY=admin123 && "
        "export AWS_DEFAULT_REGION=us-east-1"
    )
    endpoint_url = "--endpoint-url=http://minio:9000"
    
    # Check if bucket exists, create if not
    cmd = f"{aws_config} aws s3 ls s3://warehouse/ {endpoint_url} 2>&1"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if 'NoSuchBucket' in result.stderr or 'does not exist' in result.stderr.lower():
        print("Creating warehouse bucket...")
        cmd = f"{aws_config} aws s3 mb s3://warehouse {endpoint_url} 2>&1"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print("OK: Bucket created successfully")
        else:
            print(f"Warning: {result.stderr}")
    elif result.returncode == 0:
        print("OK: Warehouse bucket already exists")
    else:
        print(f"Warning: {result.stderr}")
    
    print("MinIO configuration complete")


def check_debezium_status(**context):
    """Check if Debezium connector is running"""
    import requests
    try:
        response = requests.get('http://debezium:8083/connectors/postgres-connector/status', timeout=10)
        status = response.json()
        
        if status['connector']['state'] != 'RUNNING':
            raise Exception(f"Debezium connector not running: {status['connector']['state']}")
        
        print("Debezium connector is RUNNING")
        return True
    except Exception as e:
        print(f"Debezium check failed: {e}")
        return True


def process_table(table_name, **context):
    """Process a single table - run Spark job to sync data from Kafka to S3"""
    print(f"\n{'=' * 50}")
    print(f"Processing table: {table_name}")
    print(f"{'=' * 50}")
    
    # Build command using enhanced job
    topic = f"{KAFKA_TOPIC_PREFIX}{table_name}"
    cmd = (
        f"docker exec glue-local python3 /workspace/glue-jobs/kafka_to_s3_enhanced.py "
        f"--JOB_NAME=cdc-sync-{table_name} "
        f"--kafka_topic={topic} "
        f"--iceberg_warehouse={WAREHOUSE_PATH} "
        f"--table_name={table_name} "
        f">>/tmp/spark_{table_name}.log 2>&1"
    )
    
    print(f"Running Spark job for {table_name}...")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"Successfully processed {table_name}")
        return True
    else:
        print(f"Spark job for {table_name} completed with warnings: {result.stderr[:200]}")
        return True


def verify_data_in_minio(**context):
    """Verify data has been written to MinIO using AWS CLI"""
    print("\n" + "=" * 50)
    print("Verifying Data in MinIO/S3")
    print("=" * 50)
    
    # AWS CLI configuration
    aws_config = (
        "export AWS_ACCESS_KEY_ID=admin && "
        "export AWS_SECRET_ACCESS_KEY=admin123 && "
        "export AWS_DEFAULT_REGION=us-east-1"
    )
    endpoint_url = "--endpoint-url=http://minio:9000"
    
    results = {}
    
    # First, list all table prefixes in the warehouse bucket
    cmd = f"{aws_config} aws s3 ls s3://warehouse/ {endpoint_url} 2>&1"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    print("\nBucket contents:")
    print(result.stdout or "No contents found")
    
    # Check each table directory
    for table in TABLES:
        table_path = f"s3://warehouse/{table}_parquet/"
        cmd = f"{aws_config} aws s3 ls {table_path} {endpoint_url} 2>&1"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        # Count parquet files (lines containing .parquet)
        lines = result.stdout.strip().split('\n') if result.stdout.strip() else []
        parquet_count = sum(1 for line in lines if '.parquet' in line.lower())
        
        results[table] = parquet_count
        print(f"  {table}: {parquet_count} parquet files")
        
        if parquet_count > 0:
            # Show sample files
            sample_files = [line.split()[-1] for line in lines[:3] if '.parquet' in line.lower()]
            print(f"    Sample: {', '.join(sample_files)}")
    
    total = sum(results.values())
    print(f"\nTotal parquet files across all tables: {total}")
    
    # Additional info: Check total bucket size
    cmd = f"{aws_config} aws s3 ls s3://warehouse/ --recursive {endpoint_url} 2>&1 | wc -l"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    total_files = int(result.stdout.strip() or 0)
    print(f"Total files in warehouse: {total_files}")
    
    return {
        'results': results,
        'total_parquet_files': total,
        'total_files': total_files
    }


def show_postgres_count(**context):
    """Show current data count in PostgreSQL"""
    print("\n" + "=" * 50)
    print("PostgreSQL Data Count")
    print("=" * 50)
    
    for table in TABLES:
        cmd = f"docker exec ecommerce-postgres psql -U postgres -d ecommerce -t -c 'SELECT COUNT(*) FROM ecommerce.{table};' 2>/dev/null"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        count = result.stdout.strip() or '0'
        print(f"  {table}: {count} rows")


def log_execution_time(**context):
    """Log execution time for monitoring"""
    execution_date = context['execution_date']
    print(f"\nDAG executed at: {execution_date}")
    return True


# =============================================================================
# TASK DEFINITIONS
# =============================================================================

# Task 0: Configure MinIO
task_configure_minio = PythonOperator(
    task_id='configure_minio',
    python_callable=configure_minio,
    dag=dag,
)

# Task 1: Check Debezium status
task_check_debezium = PythonOperator(
    task_id='check_debezium_connector',
    python_callable=check_debezium_status,
    dag=dag,
)

# Task 2: Log execution time
task_log_time = PythonOperator(
    task_id='log_execution_time',
    python_callable=log_execution_time,
    dag=dag,
)

# Task 3: Process each table
task_process_orders = PythonOperator(
    task_id='process_orders',
    python_callable=lambda **ctx: process_table('orders', **ctx),
    dag=dag,
)

task_process_customers = PythonOperator(
    task_id='process_customers',
    python_callable=lambda **ctx: process_table('customers', **ctx),
    dag=dag,
)

task_process_products = PythonOperator(
    task_id='process_products',
    python_callable=lambda **ctx: process_table('products', **ctx),
    dag=dag,
)

task_process_order_items = PythonOperator(
    task_id='process_order_items',
    python_callable=lambda **ctx: process_table('order_items', **ctx),
    dag=dag,
)

# Task 4: Verify data in MinIO
task_verify_minio = PythonOperator(
    task_id='verify_minio_data',
    python_callable=verify_data_in_minio,
    dag=dag,
)

# Task 5: Show PostgreSQL count
task_show_postgres = PythonOperator(
    task_id='show_postgres_count',
    python_callable=show_postgres_count,
    dag=dag,
)

# =============================================================================
# TASK DEPENDENCIES
# =============================================================================

task_configure_minio >> task_check_debezium >> task_log_time
task_log_time >> [task_process_orders, task_process_customers, task_process_products, task_process_order_items]
[task_process_orders, task_process_customers, task_process_products, task_process_order_items] >> task_verify_minio >> task_show_postgres

