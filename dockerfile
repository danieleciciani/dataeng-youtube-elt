ARG AIRFLOW_VERSION=3.1.5
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

RUN mkdir -p /home/airflow/.dbt && chown -R airflow: /home/airflow/.dbt
