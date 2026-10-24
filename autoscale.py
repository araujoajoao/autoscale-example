# -*- coding: utf-8 -*-
"""
DAG de Autoescalonamento de Cluster Cloudera (CDP).

Gerencia o escalonamento automático (upscale e downscale gradual) de nós worker
com base no horário de execução, conclusão de DAGs externas e métricas de saúde do HDFS.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import urljoin

import pendulum
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from custom_functions.FindLogErrors import FindLogErrors

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClusterConfig:
    """Parâmetros e limites operacionais para escalonamento seguro do cluster."""

    MAX_BLOCKS: int = 100
    MAX_HEALTH_RATE: float = 0.05
    MIN_NODES: int = 3
    MAX_NODES: int = 30
    SCALE_DOWN_STEPS: tuple[int, ...] = (14, 8, 5, 3)
    BUSINESS_HOUR_START: int = 8
    BUSINESS_HOUR_END: int = 18
    SSH_CONN_ID: str = "default_ssh_conn_id"
    INSTANCE_GROUP: str = "worker"
    TIMEZONE: str = "America/Sao_Paulo"


def send_slack_notification(context: dict[str, Any]) -> None:
    """Envia uma notificação ao Slack quando o DAG finaliza com sucesso."""
    slack_webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var="")
    if not slack_webhook_url:
        logger.info("SLACK_WEBHOOK_URL não configurada; notificação ignorada.")
        return

    dag_id = context["dag"].dag_id
    logical_date = context.get("logical_date") or context.get("execution_date")
    message = f":white_check_mark: DAG finalizado com sucesso: `{dag_id}` (Execução: `{logical_date}`)"

    try:
        response = requests.post(
            slack_webhook_url,
            json={"text": message},
            timeout=(5, 15),
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("Falha ao enviar notificação para o Slack: %s", exc)


class ClusterMetrics:
    """Cliente para consulta de séries temporais na API do Cloudera Manager."""

    API_TIMEOUT: tuple[int, int] = (10, 30)
    TIMESERIES_ENDPOINT: str = "cdp-proxy-api/cm-api/v51/timeseries"

    @classmethod
    def fetch_metric(
        cls,
        namenode_url: str,
        api_credentials: str,
        query: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> float:
        """
        Consulta o último valor de uma métrica no Cloudera Manager.

        Raises:
            AirflowException: Em caso de erro HTTP ou falha de conectividade.
        """
        base_url = namenode_url.rstrip("/") + "/"
        url = urljoin(base_url, cls.TIMESERIES_ENDPOINT)
        headers = {
            "Authorization": api_credentials,
            "Content-Type": "application/json",
        }
        params = {
            "query": query,
            "startTime": str(start_time_ms),
            "endTime": str(end_time_ms),
        }

        try:
            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=cls.API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            items = data.get("items") or []
            if not items:
                return 0.0

            time_series = items[0].get("timeSeries") or []
            if not time_series:
                return 0.0

            data_points = time_series[0].get("data") or []
            if not data_points:
                return 0.0

            return float(data_points[-1].get("value", 0.0))

        except (requests.RequestException, ValueError) as exc:
            raise AirflowException(f"Erro ao consultar métrica na API Cloudera: {exc}") from exc


def check_hour(**context: Any) -> str:
    """
    Avalia o horário atual no fuso configurado e define o fluxo de execução.

    Returns:
        str: Task ID inicial do grupo de upscale (horário comercial) ou
             Task ID inicial do grupo de downscale (fora do horário comercial).
    """
    tz = pendulum.timezone(ClusterConfig.TIMEZONE)
    current_hour = pendulum.now(tz).hour
    logger.info("Hora atual (%s): %02d:00", ClusterConfig.TIMEZONE, current_hour)

    if ClusterConfig.BUSINESS_HOUR_START <= current_hour <= ClusterConfig.BUSINESS_HOUR_END:
        return "upscale_cluster.upscale_nodes"

    first_step = ClusterConfig.SCALE_DOWN_STEPS[0]
    return f"downscale_cluster.verify_downscale_{first_step}"


def verify_cluster_state(target_nodes: int, **context: Any) -> None:
    """
    Valida blocos sub-replicados do HDFS e a taxa de erro de saúde do cluster
    ANTES de executar cada etapa de redução de nós.

    Args:
        target_nodes: Número de nós alvo da etapa atual de downscale.

    Raises:
        AirflowException: Se qualquer métrica ultrapassar os limites de segurança.
    """
    namenode = Variable.get("NAMENODE")
    api_credentials = Variable.get("API_CREDENTIALS")
    cluster_name = Variable.get("CLUSTER_NAME")

    logger.info("Validando saúde do cluster '%s' antes do downscale para %d nós.", cluster_name, target_nodes)

    current_time_ms = int(time.time() * 1000)
    one_hour_ago_ms = current_time_ms - 3_600_000

    blocks_query = (
        f"SELECT under_replicated_blocks_across_hdfss WHERE clusterName = '{cluster_name}'"
    )
    health_query = (
        f"SELECT health_bad_rate WHERE clusterName = '{cluster_name}'"
    )

    blocks = ClusterMetrics.fetch_metric(
        namenode_url=namenode,
        api_credentials=api_credentials,
        query=blocks_query,
        start_time_ms=one_hour_ago_ms,
        end_time_ms=current_time_ms,
    )
    logger.info("Blocos sub-replicados no HDFS: %s (Limite: %s)", blocks, ClusterConfig.MAX_BLOCKS)

    health_bad_rate = ClusterMetrics.fetch_metric(
        namenode_url=namenode,
        api_credentials=api_credentials,
        query=health_query,
        start_time_ms=one_hour_ago_ms,
        end_time_ms=current_time_ms,
    )
    logger.info("Taxa de saúde ruim: %s (Limite: %s)", health_bad_rate, ClusterConfig.MAX_HEALTH_RATE)

    if blocks > ClusterConfig.MAX_BLOCKS:
        raise AirflowException(
            f"Downscale abortado para {target_nodes} nós: blocos não replicados ({blocks}) "
            f"acima do limite ({ClusterConfig.MAX_BLOCKS})."
        )

    if health_bad_rate > ClusterConfig.MAX_HEALTH_RATE:
        raise AirflowException(
            f"Downscale abortado para {target_nodes} nós: taxa de saúde ruim ({health_bad_rate}) "
            f"acima do limite ({ClusterConfig.MAX_HEALTH_RATE})."
        )

    logger.info("Cluster saudável. Autorizado o downscale para %d nós.", target_nodes)


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

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "email_on_retry": False,
    "email_on_failure": False,
    "on_failure_callback": error_email.buildEmail,
}

with DAG(
    dag_id="cluster_autoscaling_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz=ClusterConfig.TIMEZONE),
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["PRODUCTION", "AUTOSCALING", "CDP"],
    on_success_callback=send_slack_notification,
    doc_md=__doc__,
) as dag:

    t_check_hour = BranchPythonOperator(
        task_id="check_hour",
        python_callable=check_hour,
    )

    with TaskGroup(
        group_id="upscale_cluster",
        tooltip="Escalonamento para cima e aguardo de processamento externo",
    ) as upscale_group:
        upscale_nodes = SSHOperator(
            task_id="upscale_nodes",
            ssh_conn_id=ClusterConfig.SSH_CONN_ID,
            command=(
                "cdp datahub scale-cluster "
                "--cluster-name {{ var.value.CLUSTER_NAME }} "
                f"--instance-group-name {ClusterConfig.INSTANCE_GROUP} "
                f"--instance-group-desired-count {ClusterConfig.MAX_NODES}"
            ),
            execution_timeout=timedelta(minutes=15),
        )

        wait_external_workload = ExternalTaskSensor(
            task_id="wait_external_workload",
            external_dag_id="{{ var.value.get('EXTERNAL_WORKLOAD_DAG_ID', 'workload_principal_dag') }}",
            external_task_id=None,
            allowed_states=["success"],
            failed_states=["failed", "upstream_failed"],
            mode="reschedule",
            poke_interval=60,
            timeout=60 * 60 * 4,
        )

        upscale_nodes >> wait_external_workload

    with TaskGroup(
        group_id="downscale_cluster",
        tooltip="Validação de saúde e redução gradual de nós do cluster",
    ) as downscale_group:
        previous_task = None

        for step_index, nodes in enumerate(ClusterConfig.SCALE_DOWN_STEPS):
            # A primeira verificação pode ser acionada tanto após o upscale quanto diretamente pelo Branch
            step_trigger_rule = (
                TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
                if step_index == 0
                else TriggerRule.ALL_SUCCESS
            )

            verify_task = PythonOperator(
                task_id=f"verify_downscale_{nodes}",
                python_callable=verify_cluster_state,
                op_kwargs={"target_nodes": nodes},
                trigger_rule=step_trigger_rule,
                execution_timeout=timedelta(minutes=5),
            )

            downscale_task = SSHOperator(
                task_id=f"downscale_{nodes}_nodes",
                ssh_conn_id=ClusterConfig.SSH_CONN_ID,
                command=(
                    "cdp datahub scale-cluster "
                    "--cluster-name {{ var.value.CLUSTER_NAME }} "
                    f"--instance-group-name {ClusterConfig.INSTANCE_GROUP} "
                    f"--instance-group-desired-count {nodes}"
                ),
                retries=3,
                retry_delay=timedelta(minutes=2),
                execution_timeout=timedelta(minutes=10),
            )

            if previous_task:
                previous_task >> verify_task

            # Verifica a saúde do cluster ANTES de reduzir os nós
            verify_task >> downscale_task
            previous_task = downscale_task

    downscale_success = EmptyOperator(
        task_id="downscale_success",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    t_check_hour >> [upscale_group, downscale_group]
    upscale_group >> downscale_group >> downscale_success
