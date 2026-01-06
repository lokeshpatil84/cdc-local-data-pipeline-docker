#!/usr/bin/env python3

import subprocess
import sys


def run_command(cmd, description):
    """Run shell command and report status"""
    print("\n" + "=" * 50)
    print(f"{description}...")
    print("=" * 50)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"OK: {description}")
        if result.stdout.strip():
            print(result.stdout)
        return True
    else:
        print(f"ERROR: {description}")
        if result.stderr.strip():
            print(result.stderr)
        return False


def configure_minio():
    """Configure MinIO alias for mc commands"""
    print("\nConfiguring MinIO alias...")
    # Set up alias for MinIO (suppress errors if already configured)
    subprocess.run(
        "docker exec minio mc alias set local http://minio:9000 admin admin123 2>/dev/null || true",
        shell=True
    )
    # Make sure warehouse bucket exists
    subprocess.run(
        "docker exec minio mc mb -p local/warehouse 2>/dev/null || true",
        shell=True
    )


def main():
    print("""
==================================================
E-Commerce CDC Pipeline - End to End Runner
==================================================

This script will:
1. Start all Docker services (PostgreSQL, Kafka, Debezium, MinIO)
2. Create Debezium CDC connector
3. Insert sample data into PostgreSQL
4. Run Spark job to read from Kafka and write to MinIO (S3)
5. Verify data in MinIO

""")

    # Step 1: Start Docker services
    run_command(
        "cd /home/lokeshp6/Downloads/ecommerce-data-pipeline-minlo-main-cicd/docker && docker-compose up -d",
        "Starting Docker Services"
    )

    # Step 2: Wait for services
    print("\nWaiting for services to be ready (30s)...")
    subprocess.run("sleep 30", shell=True)

    # Step 3: Configure MinIO alias (before using mc commands)
    configure_minio()

    # Step 4: Check Debezium connector
    print("\nChecking if connector exists...")
    result = subprocess.run(
        "curl -s http://localhost:8083/connectors/ | grep postgres-connector",
        shell=True, capture_output=True, text=True
    )

    if "postgres-connector" not in result.stdout:
        run_command(
            "curl -X POST -H 'Content-Type: application/json' --data @/home/lokeshp6/Downloads/ecommerce-data-pipeline-minlo-main-cicd/config/debezium-config.json 'http://localhost:8083/connectors'",
            "Creating Debezium Connector"
        )
    else:
        print("OK: Debezium connector already exists")

    # Step 5: Check and insert data
    print("\nChecking PostgreSQL data...")
    result = subprocess.run(
        "docker exec ecommerce-postgres psql -U postgres -d ecommerce -t -c 'SELECT COUNT(*) FROM ecommerce.orders;'",
        shell=True, capture_output=True, text=True
    )
    count = int(result.stdout.strip() or 0)
    
    if count == 0:
        run_command(
            "docker exec ecommerce-postgres psql -U postgres -d ecommerce -c \"INSERT INTO ecommerce.orders (customer_id, total_amount, status, shipping_address) VALUES (1, 1299.99, 'pending', '123 Main St, Mumbai'), (2, 199.99, 'processing', '456 Oak Ave, Delhi');\"",
            "Inserting Sample Data"
        )

    # Step 6: Run Spark job
    run_command(
        "docker exec glue-local python3 /workspace/glue-jobs/kafka_to_s3_batch.py --JOB_NAME=kafka-to-s3-local --kafka_topic=dbserver1.ecommerce.orders --iceberg_warehouse=s3a://warehouse/",
        "Running Spark Job (Kafka -> S3)"
    )

    # Step 7: Show results
    print("""
==================================================
PIPELINE COMPLETE!
==================================================

ACCESS URLs:
   MinIO Console:     http://localhost:9001  (admin/admin123)
   Kafka UI:          http://localhost:8080
   Airflow Webserver: http://localhost:8090  (admin/admin)
   Debezium REST:     http://localhost:8083

S3 (MinIO) Data Location:
   warehouse/orders_parquet/

TO TEST CDC (Real-time data sync):
   1. Insert new data:
      docker exec ecommerce-postgres psql -U postgres -d ecommerce -c "INSERT INTO ecommerce.orders..."

   2. Run Spark job:
      docker exec glue-local python3 /workspace/glue-jobs/kafka_to_s3_batch.py --JOB_NAME=kafka-to-s3-local --kafka_topic=dbserver1.ecommerce.orders --iceberg_warehouse=s3a://warehouse/

   3. Check MinIO:
      docker exec minio mc ls -r local/warehouse/

Current Data Count:
""")
    subprocess.run(
        "docker exec ecommerce-postgres psql -U postgres -d ecommerce -c 'SELECT COUNT(*) as postgres_count FROM ecommerce.orders;'",
        shell=True
    )
    subprocess.run(
        "docker exec minio mc ls local/warehouse/orders_parquet/ 2>/dev/null | grep -c 'parquet' || echo '0 parquet files'",
        shell=True
    )
    print("")


if __name__ == "__main__":
    main()

