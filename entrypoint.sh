#!/bin/bash

# Initialize the Airflow database
airflow db init

# Create default user
airflow users create -r Admin -u admin -p admin -e admin@example.com -f Admin -l User

# Start the scheduler in the background
airflow scheduler &

# Start the webserver
exec airflow webserver
