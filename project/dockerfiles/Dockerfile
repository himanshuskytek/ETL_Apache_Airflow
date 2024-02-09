# FROM puckel/docker-airflow:1.10.9

FROM puckel/docker-airflow:1.10.9

USER root

# Install required system packages
RUN apt-get update \
    && apt-get install -y libaio1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install cx_Oracle
RUN pip install cx_Oracle

USER airflow


