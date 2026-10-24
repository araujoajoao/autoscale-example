# Cluster Autoscaling DAG Documentation

## Overview

This DAG (`cluster_autoscaling_dag`) manages the automatic scaling of Cloudera Data Platform (CDP) DataHub worker nodes based on execution time, external workload completion, and real-time cluster health metrics. Built on Apache Airflow 2.x, it monitors HDFS block replication and cluster health rates before stepping down node counts.

## Key Features

- **Dynamic Cluster Scaling**: Scales worker nodes up to peak capacity during business hours or steps them down gradually during off-peak periods.
- **Pre-Downscale HDFS Health Verification**: Validates HDFS under-replicated blocks and cluster health error rates *before* each downscale step to prevent data loss or instability.
- **Lazy Variable Evaluation**: Reads Airflow Variables at task runtime or via Jinja templates (`{{ var.value.* }}`), avoiding top-level database queries during scheduler DAG parsing.
- **External Workload Synchronization**: Uses `ExternalTaskSensor` (in `reschedule` mode) after upscaling so worker slots are freed while waiting for heavy processing DAGs to finish.
- **Automated Alerting**: Sends failure alerts via `FindLogErrors` email callbacks and completion notifications to Slack via webhook.

## Architecture & DAG Flow

1. **Branching by Business Hours (`check_hour`)**
   - Evaluates the current hour in the configured timezone (`America/Sao_Paulo`, 08:00–18:00):
     - **08:00 to 18:00 (Business Hours)**: Branches to `upscale_cluster.upscale_nodes`.
     - **Off-Peak Hours**: Branches directly to `downscale_cluster.verify_downscale_14` to safely reduce cluster capacity.
2. **Upscale & Workload Wait (`upscale_cluster`)**
   - **`upscale_nodes`**: Scales the `worker` instance group to `MAX_NODES` (`30`) via the CDP CLI over SSH.
   - **`wait_external_workload`**: Waits for the target processing DAG (`EXTERNAL_WORKLOAD_DAG_ID`) to complete before initiating the downscale sequence.
3. **Gradual Downscale (`downscale_cluster`)**
   - Iterates through predefined node thresholds (`14 → 8 → 5 → 3`).
   - For each step:
     1. **`verify_downscale_<N>`**: Queries Cloudera Manager timeseries metrics (`under_replicated_blocks_across_hdfss` and `health_bad_rate`) over the last 60 minutes. Aborts with `AirflowException` if thresholds are exceeded.
     2. **`downscale_<N>_nodes`**: Executes the `cdp datahub scale-cluster` command to reduce the worker count to `<N>`.
4. **Completion (`downscale_success`)**
   - Consolidates execution paths using `TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS` and triggers the `send_slack_notification` callback on DAG completion.

## Configuration

### Airflow Variables

| Variable Name | Required | Description |
| :--- | :--- | :--- |
| `NAMENODE` | Yes | Base URL of the Cloudera Manager / HDFS NameNode proxy API. |
| `CLUSTER_NAME` | Yes | Name of the CDP DataHub cluster to scale. |
| `API_CREDENTIALS` | Yes | `Authorization` header value for Cloudera Manager API requests. |
| `EXTERNAL_WORKLOAD_DAG_ID` | No | External DAG ID to wait for after upscaling (defaults to `workload_principal_dag`). |
| `SLACK_WEBHOOK_URL` | No | Incoming Webhook URL for Slack success notifications. |

### Scaling Parameters (`ClusterConfig`)

| Parameter | Default Value | Description |
| :--- | :--- | :--- |
| `MAX_BLOCKS` | `100` | Maximum allowed under-replicated HDFS blocks before a downscale step. |
| `MAX_HEALTH_RATE` | `0.05` | Maximum allowed cluster `health_bad_rate` (5%) before a downscale step. |
| `MAX_NODES` | `30` | Target worker node count during upscaling. |
| `MIN_NODES` | `3` | Minimum worker node count after full downscaling. |
| `SCALE_DOWN_STEPS` | `(14, 8, 5, 3)` | Sequential worker counts applied during gradual downscaling. |
| `BUSINESS_HOUR_START` | `8` | Start hour (24h format) for the upscale window. |
| `BUSINESS_HOUR_END` | `18` | End hour (24h format) for the upscale window. |

## Core Components

### `ClusterMetrics.fetch_metric(namenode_url, api_credentials, query, start_time_ms, end_time_ms)`
Queries `/cdp-proxy-api/cm-api/v51/timeseries` on Cloudera Manager and safely extracts the latest metric value, returning `0.0` when time series arrays are empty and raising `AirflowException` on network or HTTP errors.

### `verify_cluster_state(target_nodes, **context)`
Runs prior to each `SSHOperator` downscale task. Fetches HDFS replication and cluster health metrics for the past hour and blocks scaling if `blocks > MAX_BLOCKS` or `health_bad_rate > MAX_HEALTH_RATE`.

### `send_slack_notification(context)`
DAG-level `on_success_callback` that posts the DAG ID and logical execution date to Slack when `SLACK_WEBHOOK_URL` is configured.

## Dependencies

- `apache-airflow>=2.4`
- `apache-airflow-providers-ssh`
- `requests`
- `pendulum`
