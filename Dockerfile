#https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html

FROM apache/airflow:2.2.3
ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update


FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python"]

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]