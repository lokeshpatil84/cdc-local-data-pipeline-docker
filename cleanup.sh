#!/bin/bash
# Stop all Docker containers and services

echo "Stopping all Docker services..."

cd docker

# Stop and remove all containers WITH volumes (to avoid stale data issues)
docker compose down -v

# Also remove any orphaned containers
docker ps -aq --filter "name=ecommerce" --filter "name=kafka" --filter "name=debezium" --filter "name=minio" --filter "name=airflow" --filter "name=glue" | xargs -r docker rm -f 2>/dev/null || true

echo ""
echo "All services stopped and volumes cleaned!"
echo ""
echo "Stopped containers:"
echo "   - ecommerce-postgres (5432)"
echo "   - kafka (9092)"
echo "   - debezium (8083)"
echo "   - kafka-ui (8080)"
echo "   - airflow-webserver (8090)"
echo "   - airflow-scheduler"
echo "   - minio (9000, 9001)"
echo ""
echo "Volumes have been cleaned. Next run will start fresh."

