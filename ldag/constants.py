from string import Template

DAG_ID = "dag_id"
DAG_PARAMS = "dag_params"
RUN_ID = "run_id"
STATUS = "status"
TASK_ID = "task_id"

DAG_TRIGGER_QUEUE_NAME_TEMPLATE = Template("$app_id-dags-queue")
TASKS_QUEUE_NAME_TEMPLATE = Template("$app_id-tasks-queue")
TASKS_STATUS_QUEUE_NAME_TEMPLATE = Template("$app_id-tasks-status-queue")
