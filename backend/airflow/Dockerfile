FROM apache/airflow:2.7.1

USER root

# Install packages and docker sock
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Permissions to the user airflow
RUN groupadd -f docker && usermod -aG docker airflow


USER airflow

# Extra libraries
# RUN pip install --no-cache-dir pandas requests