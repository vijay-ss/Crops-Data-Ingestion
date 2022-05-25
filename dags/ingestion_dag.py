import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from urllib.parse import urlparse
import re

import pandas as pd
import numpy as np
import openpyxl
import lxml

from ingestion_usda_files import download_usda_files
from ingestion_usda_files import usda_file_urls
from transfrom_data import transform_callable

# enable logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('dag_runs.log')
file_handler.setFormatter((formatter))

logger.addHandler((file_handler))


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 5, 1),
    "depends_on_past": False,
    "retries": 1,
    "email_on_failure": True,
    "email": ['####@gmail.com'],
    "sla": timedelta(minutes=10)
}

with DAG(
    dag_id="usda_crops_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['de-crops'],
) as dag:

    ingest_usda_files_task = PythonOperator(
        task_id="ingest_usda_files",
        python_callable=download_usda_files,
        op_kwargs={
            "url_list": usda_file_urls
        }
    )

    data_process_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_callable
    )

ingest_usda_files_task >> data_process_task