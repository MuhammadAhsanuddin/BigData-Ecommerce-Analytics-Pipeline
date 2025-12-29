#!/bin/bash

echo "Starting Airflow database migration..."
airflow db migrate

echo "Database migration complete!"

echo "Creating admin user..."
# Try to create user - ignore errors if user already exists
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>&1 || echo "User creation completed (user may already exist)"

echo "Airflow initialization complete!"
exit 0