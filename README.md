# Airflow DAG: Cluster Scaling Automation

## Overview

This Airflow DAG automates the scaling of a data cluster based on various metrics and conditions. It integrates with Cloudera Data Platform (CDP) to ensure efficient resource utilization by performing tasks such as:

Monitoring HDFS Block Replication: Tracks under-replicated blocks in HDFS to ensure data redundancy.
Cluster Health Check: Monitors the cluster's health status to detect anomalies.
Dynamic Cluster Scaling: Performs upscaling or downscaling operations based on predefined thresholds and time-based conditions.
Error Notification: Automatically notifies the responsible team in case of errors.
The DAG leverages Python functions, CDP's API, and SSH commands to manage cluster operations.

## Requirements

Software
Apache Airflow: A running instance of Apache Airflow to execute the DAG.
Cloudera Data Platform (CDP): For managing and interacting with the data cluster.
Python Packages
requests: For making HTTP requests to CDP's API.
datetime: For time-based operations and scheduling.
logging: For detailed logging of DAG operations.
Airflow Providers
apache-airflow-providers-ssh: For executing SSH commands during cluster scaling.
apache-airflow-providers-http: For interacting with CDP's API.
Variables

To ensure seamless execution, the following Airflow Variables must be configured in the Airflow UI or CLI:

## Variable Name	Description

NAMENODE_HOST	Host address of the NameNode.
CLUSTER_NAME	Name of the cluster in CDP.
API_CREDENTIALS	API authorization token for CDP.
BUCKET_INTERMEDIARY	Path to the intermediary GCS bucket.
Functional Details

## Scaling Logic

Upscaling: The DAG upscales the cluster to a desired number of nodes if the current time is greater than or equal to 10:00 AM.
Downscaling: The DAG sequentially reduces the cluster size in predefined steps (e.g., 14, 8, 5, 3 nodes) based on cluster health and HDFS replication metrics.
Health and Safety Checks
HDFS Block Monitoring: If under-replicated blocks exceed a threshold (e.g., 100), downscaling is aborted.
Cluster Health: If the bad health rate exceeds 5%, downscaling is halted.
Error Notification
The DAG integrates the FindLogErrors utility to notify stakeholders in case of failures. This ensures that issues are addressed promptly.

## Example Configuration:
```python
error_email = FindLogErrors(
    email="alerts@company.com",
    id_user="123456789",
    user_name="Operations Team",
    urgency=0,
    tags=[5],
    responsible_team="Data Engineering Squad",
    bitbucket_repo="https://bitbucket.org/company/repo_name/",
    sla="2 hours",
    emails_to_send=["alerts@company.com", "team@company.com"],
    cc_email="operations@company.com",
)
```

## Workflow Overview

Check Current Time
The DAG determines whether to initiate upscaling or proceed to downscaling based on the current time.
Upscaling Process
A predefined number of worker nodes (e.g., 30) are added to the cluster using SSH commands.
Inter-DAG Dependencies
The DAG uses an external task sensor to wait for successful completion of another DAG (IFRS9_90_DAYS) before proceeding with operations.
Downscaling Process
Nodes are reduced sequentially in steps (e.g., 14 → 8 → 5 → 3), ensuring that conditions for safe downscaling are met.
Success Completion
On successful execution, the DAG completes with a downscale_success marker.
Additional Notes

## Execution Parameters: The DAG is configured to avoid overlapping runs (max_active_runs=1) and does not backfill (catchup=False).
Scaling Commands: CDP scaling operations are executed via SSH using commands like:

cdp datahub scale-cluster --cluster-name CLUSTER_NAME --instance-group-name worker --instance-group-desired-count X

Replace CLUSTER_NAME and X with appropriate values.
Logging and Debugging: The DAG includes detailed logging to facilitate debugging and audit trails.
