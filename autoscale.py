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
NAMENODE = Variable.get("NAMENODE_HOST", "default_namenode_host")
CLUSTER_NAME = Variable.get("CLUSTER_NAME", "default_cluster_name")
API_CREDENTIALS = Variable.get("API_CREDENTIALS", "default_api_credentials")
bucket_path = Variable.get("BUCKET_INTERMEDIARY", "default_bucket_path")
project_path = f"gs://{bucket_path}/repo_name"

API_TIMEOUT = (10, 30)  # Timeout in seconds for API requests

# Initial validation of required variables
if not NAMENODE or not CLUSTER_NAME or not API_CREDENTIALS:
    raise AirflowException("Missing required configurations.")

# Email configuration for error notification
error_email = FindLogErrors(
    email="default@company.com",
    id_user="123456789",
    user_name="Default User",
    urgency=0,
    tags=[0],
    responsible_team="Default Team",
    bitbucket_repository="https://bitbucket.org/default_repo",
    support_period="09:00 - 18:00",
    sla="2 Hours",
    emails_to_send=[
        "email1@company.com",
        "email2@company.com",
    ],
    cc_email="default@company.com",
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

# Function to check the current hour
def check_hour():
    current_time = datetime.now()
    if current_time.hour >= 10:
        logging.info("Current hour >= 10h. Scaling up.")
        return "upscale_cluster.upscale_nodes"
    else:
        logging.info("Current hour < 10h. Redirecting to downscale_success.")
        return "downscale_success"

# Function to get the last execution of the "dag id" DAG
def get_execution_date_dag_id(**kwargs):
    dag_id = "dag_id_name"
    try:
        with create_session() as session:
            dag_last_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag_id)
                .filter(DagRun.state == "success")
                .order_by(DagRun.execution_date.desc())
                .first()
            )
            if dag_last_run:
                execution_date = dag_last_run.execution_date
                logging.info(f"[Task {kwargs['task_id']}] Last execution of DAG '{dag_id}': {execution_date}")
                return execution_date
            logging.warning(f"[Task {kwargs['task_id']}] No successful execution found for DAG '{dag_id}'.")
            return None
    except Exception as e:
        raise AirflowException(f"Error fetching last execution of DAG '{dag_id}': {e}")

# Function to fetch HDFS block metrics
def get_hdfs_blocks():
    url = os.path.join(NAMENODE, "cdp-proxy-api/cm-api/v51/timeseries")
    query = f"select under_replicated_blocks_across_hdfss WHERE clusterName = '{CLUSTER_NAME}'"
    current_time = int(time.time() * 1000)
    start_time = current_time - 3600000
    params = {
        "query": query,
        "startTime": str(start_time),
        "endTime": str(current_time),
    }
    headers = {
        "Authorization": API_CREDENTIALS,
        "Content-Type": "application/json",
    }
    try:
        response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        if items:
            data_points = items[0].get("timeSeries", [])[0].get("data", [])
            last_value = data_points[-1].get("value", 0) if data_points else 0
            logging.info(f"Under-replicated HDFS blocks: {last_value}")
            return last_value
        return 0
    except requests.RequestException as e:
        raise AirflowException(f"API connection error: {e}")

# Function to perform health checks
def get_health_check():
    url = os.path.join(NAMENODE, "cdp-proxy-api/cm-api/v51/timeseries")
    query = f"select health_bad_rate WHERE clusterName = '{CLUSTER_NAME}'"
    current_time = int(time.time() * 1000)
    start_time = current_time - 3600000
    params = {
        "query": query,
        "startTime": str(start_time),
        "endTime": str(current_time),
    }
    headers = {
        "Authorization": API_CREDENTIALS,
        "Content-Type": "application/json",
    }
    try:
        response = requests.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        if items:
            data_points = items[0].get("timeSeries", [])[0].get("data", [])
            last_value = data_points[-1].get("value", 0) if data_points else 0
            logging.info(f"Health check metric: {last_value}")
            return last_value
        return 0
    except requests.RequestException as e:
        raise AirflowException(f"API connection error: {e}")

# Function to verify cluster state before scaling down nodes
def verify_cluster_state(nodes, **kwargs):
    blocks = get_hdfs_blocks()
    health = get_health_check()
    if blocks > 100 or health > 0.05:
        raise AirflowException(
            f"Unsafe conditions for downscale: blocks={blocks}, health={health}"
        )
    logging.info(f"Conditions verified for downscale to {nodes} nodes.")
    return "downscale_success"

# Define the DAG
with DAG(
    "dag_name",
    start_date=datetime(2024, 9, 16),
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["PRODUCTION", "MONTHLY"],
) as dag:
    t_check_hour = BranchPythonOperator(
        task_id="check_hour",
        python_callable=check_hour,
    )

    with TaskGroup("upscale_cluster") as upscale_group:
        upscale_nodes = SSHOperator(
            task_id="upscale_nodes",
            ssh_conn_id="default_ssh_conn_id",
            command=f"cdp datahub scale-cluster --cluster-name {CLUSTER_NAME} --instance-group-name worker --instance-group-desired-count 30",
        )

    t_task_sensor = ExternalTaskSensor(
        task_id="task_sensor",
        external_dag_id="DEFAULT_EXTERNAL_DAG_ID",
        external_task_id="DEFAULT_EXTERNAL_TASK_ID",
        allowed_states=["success"],
        mode="poke",
        timeout=1800,
    )

    t_get_execution_dag_id_level = PythonOperator(
        task_id="get_execution_dag_id_level",
        python_callable=get_execution_dag_id_level,
        provide_context=True,
    )

    t_hdfs_data = PythonOperator(
        task_id="get_hdfs_blocks",
        python_callable=get_hdfs_blocks,
        provide_context=True,
    )

    t_health_check = PythonOperator(
        task_id="get_health_check",
        python_callable=get_health_check,
        provide_context=True,
    )

    with TaskGroup("downscale_cluster") as downscale_group:
        previous_task = None
        for nodes in [14, 8, 5, 3]:
            downscale = SSHOperator(
                task_id=f"downscale_{nodes}_nodes",
                ssh_conn_id="default_ssh_conn_id",
                command=f"cdp datahub scale-cluster --cluster-name {CLUSTER_NAME} --instance-group-name worker --instance-group-desired-count {nodes}",
            )
            verify_task = PythonOperator(
                task_id=f"verify_downscale_{nodes}",
                python_callable=verify_cluster_state,
                op_args=[nodes],
                provide_context=True,
            )
            if previous_task:
                previous_task >> downscale
            downscale >> verify_task
            previous_task = verify_task

    success = EmptyOperator(task_id="downscale_success")

    t_check_hour >> [upscale_group, success]
    upscale_group >> t_task_sensor
    t_task_sensor >> t_get_execution_date_dag_id
    t_get_execution_date_dag_id >> [t_hdfs_data, t_health_check]
    t_hdfs_data >> downscale_group
    t_health_check >> downscale_group
    downscale_group >> success
