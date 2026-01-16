#!/bin/bash

# Database connection details
DB_NAME="retaildb"
DB_USER="retail"
DB_HOST="postgres"  # This should match the service name in your docker-compose.yml
DB_PORT="5432"
DB_PASSWORD="retailpass"

# Export the password so psql can use it
export PGPASSWORD=$DB_PASSWORD

while true; do
    echo "---- $(date) : Eseguo update_shelves.sql"
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f /app/update_shelves.sql

    echo "---- $(date) : Eseguo update_pos.sql"
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f /app/update_sales.sql

    echo "---- $(date) : Eseguo update_foot_traffic.sql"
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f /app/update_foot_traffic.sql

    echo "---- $(date) : Eseguo update_alerts.sql"
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f /app/update_alerts.sql

    echo "---- $(date) : Eseguo update_warehouse_and_refill.sql"
    psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f /app/update_warehouse_and_refill.sql

    # Sleep for 60 seconds
    sleep 60
done
