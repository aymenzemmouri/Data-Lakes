FROM apache/airflow:2.7.1

USER root

# Installation des dépendances système si nécessaire
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copie et installation des requirements
COPY requirements.txt /opt/airflow/build/reqs.txt
RUN pip install -r /opt/airflow/build/reqs.txt