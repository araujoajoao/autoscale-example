# Cluster Autoscaling DAG Documentation

## Overview
This DAG manages the **automatic scaling of cluster nodes** based on time and cluster health metrics. It uses **Apache Airflow** to perform tasks such as monitoring HDFS replication and cluster health, and executing upscaling or downscaling operations. 

### Key Features
- **Dynamic Cluster Scaling**: Automatically scales nodes up or down based on predefined conditions.
- **HDFS Health Monitoring**: Validates HDFS block replication and cluster health before scaling.
- **Error Handling**: Integrates with custom notification systems to alert on failures.
- **Success Notifications**: Sends success messages to Slack when the DAG completes successfully.
- **Task Grouping**: Organizes tasks into logical groups for better readability and management.

---

## DAG Structure

### 1. **Validation and Scheduling**
- Validates environment variables (`NAMENODE`, `CLUSTER_NAME`, `API_CREDENTIALS`) before execution.
- Dynamically sets the start date as 1 day prior to the current date.
- Runs with a **manual trigger** (`schedule_interval=None`) and only allows one active run at a time (`max_active_runs=1`).

### 2. **Task Groups**
#### **Upscale Cluster**
- Scales the cluster to its maximum number of nodes (`MAX_NODES`) using the Cloudera Data Platform (CDP) `scale-cluster` command.
- Ensures sufficient resources are allocated during high-load periods.

#### **Downscale Cluster**
- Scales down the cluster in predefined steps (`SCALE_DOWN_STEPS`).
- Verifies health and HDFS block replication before each downscale step to prevent unsafe operations.

### 3. **Notification System**
- **On Failure**: Sends error notifications via email using the `FindLogErrors` system.
- **On Success**: Sends a success message to Slack using the provided webhook URL.

---

## Key Components

### **Environment Variables**
The DAG depends on the following environment variables:
- `NAMENODE`: URL of the HDFS NameNode.
- `CLUSTER_NAME`: Name of the cluster to manage.
- `API_CREDENTIALS`: Credentials for API authentication.
- `BUCKET_INTERMEDIARY`: GCS bucket path for intermediary files.
- `SLACK_WEBHOOK_URL` (Optional): Webhook URL for sending Slack notifications.

### **Configurations**
The DAG uses the `ClusterConfig` class to manage scaling parameters:
- `MAX_BLOCKS`: Maximum number of under-replicated HDFS blocks allowed before scaling down.
- `MAX_HEALTH_RATE`: Maximum cluster health error rate.
- `SCALE_DOWN_STEPS`: Steps for downscaling nodes (e.g., 14 → 8 → 5 → 3).
- `MAX_NODES`: Maximum nodes for upscaling.
- `MIN_NODES`: Minimum nodes after scaling down.

---

## Functions

### **Metrics Fetching**
#### `ClusterMetrics.fetch_metric(query, cluster_name, start_time, end_time)`
- A generic function to query metrics from the Cloudera Manager API.
- **Input**: Query string, cluster name, and time range (start and end time in milliseconds).
- **Output**: Latest value of the requested metric.

### **Scaling Verification**
#### `verify_cluster_state(nodes, **kwargs)`
- Ensures the cluster's HDFS blocks and health are within safe thresholds before scaling down.
- Raises an `AirflowException` if conditions are unsafe.

### **Slack Notifications**
#### `send_slack_notification(**kwargs)`
- Sends a success message to a Slack channel using the webhook URL provided in the `SLACK_WEBHOOK_URL` variable.

---

## DAG Flow

1. **Branching by Hour**
   - Checks the current hour to decide:
     - **If after 10 AM**: Proceed to upscale the cluster.
     - **If before 10 AM**: Skip to post-downscale validation.
2. **Upscale**
   - Executes the upscale operation to the maximum number of nodes (`MAX_NODES`).
   - Waits for an external DAG to complete before continuing.
3. **Metrics Collection**
   - Queries HDFS under-replicated blocks and health error rate.
4. **Downscale**
   - Sequentially scales down nodes in steps.
   - Verifies HDFS and cluster health metrics at each step.
5. **Completion**
   - Ends with a success task and sends a Slack notification.

---

## Dependencies
### External
- Requires an external DAG to complete successfully before proceeding to cluster scaling operations.

### Python Packages
- `requests`: For API calls and Slack notifications.
- `airflow.providers.ssh.operators.ssh.SSHOperator`: For executing CDP commands.
- `airflow.sensors.external_task_sensor.ExternalTaskSensor`: For monitoring external DAGs.

---

## Example Use Cases
- **Hadoop Workloads**: Autoscaling a Hadoop cluster to optimize resource usage during peak and off-peak hours.
- **Cost Optimization**: Ensures the cluster runs with minimal nodes during low-demand periods while maintaining safety thresholds.

---

## Improvements and Extensibility
1. **Additional Metrics**:
   - Add more metrics for comprehensive cluster health checks (e.g., CPU utilization, memory usage).
2. **Error Reporting**:
   - Integrate with other notification systems like Microsoft Teams or PagerDuty.
3. **Dynamic Schedule**:
   - Allow dynamic schedules based on workload patterns.

---

## Conclusion
This DAG provides a robust framework for **cluster autoscaling** in a **Cloudera environment**. It ensures operational safety, optimizes resource usage, and provides flexibility to adapt to various workloads.