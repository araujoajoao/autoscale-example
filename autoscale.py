# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime, timedelta
import logging
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.session import create_session
from airflow.models.dag import DagRun
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from custom_functions.FindLogErrors import FindLogErrors

# Variables
NAMENODE = Variable.get("NAMENODE_HOST")
API_USER = Variable.get("CDP_USER")
API_PASSWORD = Variable.get("CDP_PASSWORD")
CLUSTER_NAME = Variable.get("CLUSTER_NAME")
bucket_path = Variable.get("BUCKET_INTERMEDIARY")
project_path = f"gs://{bucket_path}/repo_name"

# Email setup for error notifications
error_email = FindLogErrors(
    email="fakemail@mail.com",
    id_user="user_id",
    user_name="username",
    urgency=0,
    tags=[5],
    bitbucket_repo="https://bitbucket.org/organisation/repo_name/",
    sla="2 hours",
    emails_to_send=[
        "fakemail@mail.com",
    ],
    email_copia="fakemail@mail.com",
)

# Default DAG arguments
default_args = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "email_on_retry": False,
    "email_on_failure": False,
    "on_failure_callback": error_email.buildEmail,
}

# Function to check the current hour and decide the next task
def check_hour():
    current_time = datetime.now()
    return "upscale_cluster.upscale_nodes" if current_time.hour >= 10 else "downscale_success"

# Function to fetch the last execution date of the "nivel_risco" DAG
def get_execution_date_dag_name(**kwargs):
    dag_id = "dag_name"
    try:
        with create_session() as session:
            dag_a_last_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag_id)
                .filter(DagRun.state == "success")
                .order_by(DagRun.execution_date.desc())
                .first()
            )
            
            if dag_a_last_run:
                execution_date = dag_a_last_run.execution_date
                logging.info(f"Last execution date of DAG '{dag_id}': {execution_date}")
                return execution_date
            return logging.error(f"Error fetching the last execution date for DAG '{dag_id}': {e}")
    except requests.RequestException as e:
        raise AirflowException(f"Error fetching the last execution date for DAG '{dag_id}': {e}")

# Function to retrieve HDFS block metrics
def get_hdfs_blocks():
    url = os.path.join(NAMENODE, "cdp-proxy-api/cm-api/v51/timeseries")
    query = f"select under_replicated_blocks_across_hdfss WHERE clusterName = '{CLUSTER_NAME}'"
    current_time = int(time.time() * 1000)
    start_time = current_time - 3600000
    params = {"query": query, "startTime": str(start_time), "endTime": str(current_time)}

    try:
        response = requests.get(url, params=params, auth=(API_USER, API_PASSWORD), timeout=(5, 15))
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        if items:
            data_points = items[0].get("timeSeries", [])[0].get("data", [])
            last_value = data_points[-1].get("value", 0) if data_points else 0
            logging.info(f"Under-replicated HDFS blocks: {last_value}")
            return last_value
        return None
    except requests.RequestException as e:
        raise AirflowException(f"API connection error: {e}")

# Function to perform health check
def get_health_check():
    url = os.path.join(NAMENODE, "cdp-proxy-api/cm-api/v51/timeseries")
    query = f"select health_bad_rate WHERE clusterName = '{CLUSTER_NAME}'"
    current_time = int(time.time() * 1000)
    start_time = current_time - 3600000
    params = {"query": query, "startTime": str(start_time), "endTime": str(current_time)}

    try:
        response = requests.get(url, params=params, auth=(API_USER, API_PASSWORD), timeout=(5, 15))
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        if items:
            data_points = items[0].get("timeSeries", [])[0].get("data", [])
            last_value = data_points[-1].get("value", 0) if data_points else 0
            logging.info(f"Health check metric: {last_value}")
            return last_value
        return None
    except requests.RequestException as e:
        raise AirflowException(f"API connection error: {e}")

# Define the DAG
with DAG(
    "dag_name",
    start_date=datetime(1900, 1, 1),
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["TAG"],
) as dag:

    t_check_hour = BranchPythonOperator(
        task_id="check_hour",
        python_callable=check_hour,
    )

    with TaskGroup("upscale_cluster") as upscale_group:
        upscale_nodes = SSHOperator(
            task_id="upscale_nodes",
            ssh_conn_id="test-ssh",
            command=f"cdp datahub scale-cluster --cluster-name {CLUSTER_NAME} --instance-group-name worker --instance-group-desired-count 30"
        )

    t_dag__sensor = ExternalTaskSensor(
        task_id="dag_id",
        external_dag_id="dag_name",
        external_task_id="task_name",
        allowed_states=["success"],
        mode="poke",
        timeout=1800,
    )

    t_get_execution_date_dag_name = PythonOperator(
        task_id="get_execution_date_dag_name",
        python_callable=get_execution_date_dag_name,
        provide_context=True,
    )

    t_hdfs_data = PythonOperator(
        task_id="get_hdfs_blocks",
        python_callable=get_hdfs_blocks,
    )

    t_health_check = PythonOperator(
        task_id="get_health_check",
        python_callable=get_health_check,
    )

    with TaskGroup("downscale_cluster") as downscale_group:
        previous_task = None
        for nodes in [14, 8, 5, 3]:
            downscale = SSHOperator(
                task_id=f"downscale_{nodes}_nodes",
                ssh_conn_id="conn_id",
                command=f"cdp datahub scale-cluster --cluster-name {CLUSTER_NAME} --instance-group-name worker --instance-group-desired-count {nodes}"
            )
            check_blocks = PythonOperator(task_id=f"check_blocks_{nodes}", python_callable=get_hdfs_blocks)
            health_check = PythonOperator(task_id=f"health_check_{nodes}", python_callable=get_health_check)
            if previous_task:
                previous_task >> downscale
            downscale >> check_blocks >> health_check
            previous_task = health_check

    success = EmptyOperator(task_id="downscale_success")

    t_check_hour >> upscale_group >> t_dag__sensor
    t_dag__sensor >> t_get_execution_date_dag_name >> t_hdfs_data
    t_hdfs_data >> downscale_group >> success
