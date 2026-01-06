#!/bin/bash
# Exit on error
set -e

# Simple colors for basic highlights
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$SCRIPT_DIR/docker"
CONFIG_DIR="$SCRIPT_DIR/config"

# Simple header print function
print_header() {
    echo ""
    echo "=================================================="
    echo "$1..."
    echo "=================================================="
    echo ""
}

print_success() {
    echo -e "${GREEN}OK: $1${NC}"
    if [ -n "$2" ]; then
        echo "$2"
    fi
}

print_error() {
    echo -e "${RED}ERROR: $1${NC}"
    if [ -n "$2" ]; then
        echo "$2"
    fi
}

print_info() {
    echo -e "${YELLOW}Note: $1${NC}"
}

# Function to run command and check status
run_cmd() {
    local description="$1"
    local cmd="$2"

    print_header "$description"

    if eval "$cmd"; then
        print_success "$description"
        return 0
    else
        print_error "$description"
        return 1
    fi
}

# Configure MinIO alias
configure_minio() {
    print_header "Configuring MinIO alias"

    print_info "Setting MinIO alias..."
    docker exec minio mc alias set local http://minio:9000 admin admin123 2>/dev/null || true

    # Make sure warehouse bucket exists
    print_info "Creating warehouse bucket..."
    docker exec minio mc mb -p local/warehouse 2>/dev/null || true

    print_success "MinIO alias configured"
}

# Check if connector exists
check_connector_exists() {
    local response
    response=$(curl -s http://localhost:8083/connectors/ 2>/dev/null || echo "")
    if echo "$response" | grep -q "postgres-connector"; then
        return 0
    else
        return 1
    fi
}

# Main function
main() {
    echo ""
    echo "=================================================="
    echo "E-Commerce CDC Pipeline - End to End Runner"
    echo "=================================================="
    echo ""
    echo "This script will:"
    echo "1. Start all Docker services (PostgreSQL, Kafka, Debezium, MinIO)"
    echo "2. Create Debezium CDC connector"
    echo "3. Insert sample data into PostgreSQL"
    echo "4. Run Spark job to read from Kafka and write to MinIO (S3)"
    echo "5. Verify data in MinIO"
    echo ""

    # Step 1: Start Docker services
    print_header "Starting Docker Services"
    print_info "Running: docker-compose up -d"
    cd "$DOCKER_DIR" && docker-compose up -d

    # Step 2: Wait for services
    print_info "Waiting for services to be ready (30s)..."
    sleep 30

    # Step 3: Configure MinIO alias (before using mc commands)
    configure_minio

    # Step 4: Check and create Debezium connector
    print_header "Checking Debezium Connector"
    if check_connector_exists; then
        print_success "Debezium connector already exists"
    else
        print_info "Creating Debezium connector..."
        curl -X POST -H 'Content-Type: application/json' \
            --data @"$CONFIG_DIR/debezium-config.json" \
            'http://localhost:8083/connectors'
        print_success "Debezium connector created"
    fi

    # Step 5: Check and insert data
    print_header "Checking PostgreSQL Data"
    local count
    count=$(docker exec ecommerce-postgres psql -U postgres -d ecommerce -t -c 'SELECT COUNT(*) FROM ecommerce.orders;' 2>/dev/null | xargs || echo "0")
    print_info "Current record count in orders table: $count"

    if [ "$count" -eq 0 ] 2>/dev/null; then
        print_info "Orders table is empty. Inserting sample data..."
        docker exec ecommerce-postgres psql -U postgres -d ecommerce -c \
            "INSERT INTO ecommerce.orders (customer_id, total_amount, status, shipping_address) VALUES (1, 1299.99, 'pending', '123 Main St, Mumbai'), (2, 199.99, 'processing', '456 Oak Ave, Delhi');"
        print_success "Sample data inserted"
    else
        print_success "Data already exists in orders table"
    fi

    # Step 6: Run Spark job
    print_header "Running Spark Job (Kafka -> S3)"
    print_info "Executing: docker exec glue-local python3 /workspace/glue-jobs/kafka_to_s3_batch.py"
    docker exec glue-local python3 /workspace/glue-jobs/kafka_to_s3_batch.py \
        --JOB_NAME=kafka-to-s3-local \
        --kafka_topic=dbserver1.ecommerce.orders \
        --iceberg_warehouse=s3a://warehouse/
    print_success "Spark job completed"

    # Step 7: Show results
    echo ""
    echo "=================================================="
    echo "PIPELINE COMPLETE!"
    echo "=================================================="
    echo ""
    echo "ACCESS URLs:"
    echo "   MinIO Console:     http://localhost:9001  (admin/admin123)"
    echo "   Kafka UI:          http://localhost:8080"
    echo "   Airflow Webserver: http://localhost:8090  (admin/admin)"
    echo "   Debezium REST:     http://localhost:8083"
    echo ""
    echo "S3 (MinIO) Data Location:"
    echo "   warehouse/orders_parquet/"
    echo ""
    echo "TO TEST CDC (Real-time data sync):"
    echo "   1. Insert new data:"
    echo "      docker exec ecommerce-postgres psql -U postgres -d ecommerce -c \"INSERT INTO ecommerce.orders...\""
    echo ""
    echo "   2. Run Spark job:"
    echo "      docker exec glue-local python3 /workspace/glue-jobs/kafka_to_s3_batch.py --JOB_NAME=kafka-to-s3-local --kafka_topic=dbserver1.ecommerce.orders --iceberg_warehouse=s3a://warehouse/"
    echo ""
    echo "   3. Check MinIO:"
    echo "      docker exec minio mc ls -r local/warehouse/"
    echo ""

    print_header "Current Data Statistics"
    echo "PostgreSQL Orders Count:"
    docker exec ecommerce-postgres psql -U postgres -d ecommerce -c 'SELECT COUNT(*) as postgres_count FROM ecommerce.orders;'

    echo ""
    echo "Parquet Files in MinIO:"
    local parquet_count
    parquet_count=$(docker exec minio mc ls local/warehouse/orders_parquet/ 2>/dev/null | grep -c 'parquet' || echo "0")
    echo "Found $parquet_count parquet files"

    echo ""
}

# Run main function
main "$@"

