import pika

from ldag.config import rabbitmq_host, rabbitmq_port, rabbitmq_user, rabbitmq_password, app_id
from ldag.constants import (
    DAG_TRIGGER_QUEUE_NAME_TEMPLATE,
    TASKS_QUEUE_NAME_TEMPLATE,
    TASKS_STATUS_QUEUE_NAME_TEMPLATE,
)

params = {
    "host": rabbitmq_host,
    "port": rabbitmq_port,
}
if rabbitmq_user and rabbitmq_password:
    params["credentials"] = pika.PlainCredentials(
        username=rabbitmq_user,
        password=rabbitmq_password,
    )

dag_trigger_queue_name = DAG_TRIGGER_QUEUE_NAME_TEMPLATE.substitute(app_id=app_id)
tasks_queue_name = TASKS_QUEUE_NAME_TEMPLATE.substitute(app_id=app_id)
tasks_status_queue_name = TASKS_STATUS_QUEUE_NAME_TEMPLATE.substitute(app_id=app_id)
connection = pika.BlockingConnection(pika.ConnectionParameters(**params))
channel = connection.channel()
