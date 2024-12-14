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
from airflow.models.dag import DagRun
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from custom_functions.FindLogErrors import FindLogErrors
from typing import Optional, List, Dict, Any

# Notificação para sucesso no Slack
def send_slack_notification(**kwargs):
    """Envia uma notificação para Slack no caso de sucesso."""
    slack_webhook_url = Variable.get("SLACK_WEBHOOK_URL", "")
    if slack_webhook_url:
        message = "DAG finalizado com sucesso: `{}`".format(kwargs['dag'].dag_id)
        try:
            response = requests.post(slack_webhook_url, json={"text": message})
            if response.status_code != 200:
                logging.warning(f"Falha ao enviar notificação Slack: {response.text}")
        except Exception as e:
            logging.error(f"Erro ao enviar notificação Slack: {e}")

# Configuração de email para erros
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

# Configurações padrão do DAG
default_args = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "email_on_retry": False,
    "email_on_failure": False,
    "on_failure_callback": error_email.buildEmail,
}

# Adicionar as variáveis faltantes
NAMENODE = Variable.get("NAMENODE")
API_CREDENTIALS = Variable.get("API_CREDENTIALS")
CLUSTER_NAME = Variable.get("CLUSTER_NAME")

# Adicionar a função check_hour
def check_hour(**context) -> str:
    """
    Verifica a hora atual e decide o branch apropriado.
    Returns:
        str: Nome do branch a ser executado
    """
    current_hour = datetime.now().hour
    if 8 <= current_hour <= 18:  # horário comercial
        return "upscale_cluster"
    return "downscale_success"

# Classe para encapsular lógica da API
class ClusterMetrics:
    """Classe para encapsular métricas do cluster."""
    API_TIMEOUT: tuple[int, int] = (10, 30)
    
    @staticmethod
    def fetch_metric(query: str, cluster_name: str, start_time: int, end_time: int) -> float:
        """Consulta a métrica usando a API."""
        url = os.path.join(NAMENODE, "cdp-proxy-api/cm-api/v51/timeseries")
        headers = {
            "Authorization": API_CREDENTIALS,
            "Content-Type": "application/json",
        }
        params = {
            "query": query,
            "startTime": str(start_time),
            "endTime": str(end_time),
        }
        try:
            response = requests.get(url, params=params, headers=headers, timeout=ClusterMetrics.API_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [])
            if items:
                data_points = items[0].get("timeSeries", [])[0].get("data", [])
                return data_points[-1].get("value", 0) if data_points else 0
            return 0
        except requests.RequestException as e:
            raise AirflowException(f"Erro na conexão com a API: {e}")

# Função para validação antes do escalonamento para baixo
def verify_cluster_state(nodes: int, **kwargs) -> str:
    """
    Verifica o estado do cluster antes do downscale.
    
    Args:
        nodes: Número de nós alvo
        **kwargs: Argumentos do contexto do Airflow
        
    Returns:
        str: Nome da próxima task
        
    Raises:
        AirflowException: Se as condições não forem seguras para downscale
    """
    logging.info(f"Iniciando verificação para downscale para {nodes} nós")
    
    current_time = int(time.time() * 1000)
    start_time = current_time - 3600000  # Última hora
    
    try:
        blocks = ClusterMetrics.fetch_metric(
            f"select under_replicated_blocks_across_hdfss WHERE clusterName = '{CLUSTER_NAME}'",
            CLUSTER_NAME,
            start_time,
            current_time
        )
        logging.info(f"Blocos não replicados: {blocks}")
        
        health = ClusterMetrics.fetch_metric(
            f"select health_bad_rate WHERE clusterName = '{CLUSTER_NAME}'",
            CLUSTER_NAME,
            start_time,
            current_time
        )
        logging.info(f"Taxa de saúde ruim: {health}")
        
        if blocks > ClusterConfig.MAX_BLOCKS:
            raise AirflowException(
                f"Número de blocos não replicados ({blocks}) excede o limite ({ClusterConfig.MAX_BLOCKS})"
            )
            
        if health > ClusterConfig.MAX_HEALTH_RATE:
            raise AirflowException(
                f"Taxa de saúde ruim ({health}) excede o limite ({ClusterConfig.MAX_HEALTH_RATE})"
            )
            
        logging.info(f"Verificação concluída com sucesso para {nodes} nós")
        return "downscale_success"
        
    except Exception as e:
        logging.error(f"Erro durante verificação: {str(e)}")
        raise

# Calcular data de início dinâmica
start_date = datetime.now() - timedelta(days=1)

# Configuração de nós do cluster
class ClusterConfig:
    MAX_BLOCKS = 100
    MAX_HEALTH_RATE = 0.05
    MIN_NODES = 3
    MAX_NODES = 30
    SCALE_DOWN_STEPS = [14, 8, 5, 3]

# Definição do DAG
with DAG(
    "dag_name",
    start_date=start_date,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["PRODUCTION", "MONTHLY"],
    on_success_callback=send_slack_notification,  # Notificação de sucesso
    doc_md="""
    # Cluster Autoscaling DAG
    
    Gerencia o escalonamento automático de clusters com base no horário e m��tricas de saúde.
    """,
) as dag:
    t_check_hour = BranchPythonOperator(
        task_id="check_hour",
        python_callable=check_hour,
    )

    with TaskGroup(
        "upscale_cluster",
        tooltip="Grupo de tarefas para escalonamento para cima"
    ) as upscale_group:
        upscale_nodes = SSHOperator(
            task_id="upscale_nodes",
            ssh_conn_id="default_ssh_conn_id",
            command=f"cdp datahub scale-cluster --cluster-name {CLUSTER_NAME} --instance-group-name worker --instance-group-desired-count {ClusterConfig.MAX_NODES}",
            execution_timeout=timedelta(minutes=10),
        )

    with TaskGroup(
        "downscale_cluster",
        tooltip="Grupo de tarefas para escalonamento para baixo"
    ) as downscale_group:
        previous_task = None
        for nodes in ClusterConfig.SCALE_DOWN_STEPS:
            downscale = SSHOperator(
                task_id=f"downscale_{nodes}_nodes",
                ssh_conn_id="default_ssh_conn_id",
                command=f"cdp datahub scale-cluster --cluster-name {CLUSTER_NAME} --instance-group-name worker --instance-group-desired-count {nodes}",
                retries=3,
                retry_delay=timedelta(minutes=2),
                execution_timeout=timedelta(minutes=5),
            )
            verify_task = PythonOperator(
                task_id=f"verify_downscale_{nodes}",
                python_callable=verify_cluster_state,
                op_args=[nodes],
                provide_context=True,
                execution_timeout=timedelta(minutes=5),
            )
            if previous_task:
                previous_task >> downscale
            downscale >> verify_task
            previous_task = verify_task

    success = EmptyOperator(task_id="downscale_success")

    t_check_hour >> [upscale_group, success]
    upscale_group >> downscale_group
    downscale_group >> success