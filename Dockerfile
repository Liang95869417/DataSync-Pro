FROM apache/airflow:2.9.3

# Install packages from requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Set the working directory
WORKDIR /opt/airflow

# Copy over the necessary files
COPY ./dags /opt/airflow/dags
COPY ./config /opt/airflow/config
COPY ./credentials /opt/airflow/credentials
COPY entrypoint.sh /entrypoint.sh


# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
