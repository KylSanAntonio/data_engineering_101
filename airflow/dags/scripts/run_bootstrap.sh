#!/bin/bash

set -e

echo "Running Airflow connection bootstrap..."

python /opt/airflow/dags/scripts/bootstrap_connections.py

echo "Bootstrap complete."