# Airflow DAG: Cluster Scaling Automation

## Overview

This Airflow DAG automates the scaling of a data cluster based on various metrics and conditions. It integrates with Cloudera Data Platform (CDP) and performs tasks such as:
- Monitoring HDFS block replication and cluster health.
- Upscaling and downscaling the cluster based on specific conditions.
- Sending notifications in case of errors.

The DAG leverages several Python functions and SSH commands to interact with the cluster, ensuring that it remains in an optimal state.

## Requirements

- **Apache Airflow**: The DAG is designed to run in an Apache Airflow environment.
- **Cloudera Data Platform (CDP)**: It uses CDP's APIs for cluster management.
- **Python Packages**: 
  - `requests` for making HTTP requests.
  - `datetime` for handling date and time.
  - `logging` for logging information.
- **Airflow Providers**:
  - `apache-airflow-providers-ssh` for SSH operations.
  - `apache-airflow-providers-http` for making HTTP requests.

## Variables

This DAG relies on several Airflow Variables, which should be set up in the Airflow UI or through the CLI:

| Variable Name         | Description                            |
|-----------------------|----------------------------------------|
| `NAMENODE_HOST`       | Host address of the NameNode           |
| `CDP_USER`            | API username for CDP                   |
| `CDP_PASSWORD`        | API password for CDP                   |
| `CLUSTER_NAME`        | Name of the cluster in CDP             |
| `BUCKET_INTERMEDIARY` | Path to the intermediary GCS bucket    |

## Email Notifications

The script includes error notification capabilities through the `FindLogErrors` function. In case of a failure, it sends an email to the specified recipients.

```python
error_email = FindLogErrors(
    email="fakemail@mail.com",
    id_user="user_id",
    user_name="username",
    urgency=0,
    tags=[5],
    bitbucket_repo="https://bitbucket.org/organisation/repo_name/",
    sla="2 hours",
    emails_to_send=["fakemail@mail.com"],
    email_copia="fakemail@mail.com",
)
