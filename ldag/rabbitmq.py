import pika

from ldag.config import rabbitmq_host, rabbitmq_port, rabbitmq_user, rabbitmq_password, app_id
from ldag.constants import TASKS_QUEUE_NAME_TEMPLATE

params = {
    "host": rabbitmq_host,
    "port": rabbitmq_port,
}
if rabbitmq_user and rabbitmq_password:
    params["credentials"] = pika.PlainCredentials(
        username=rabbitmq_user,
        password=rabbitmq_password,
    )

tasks_queue_name = TASKS_QUEUE_NAME_TEMPLATE.substitute(app_id=app_id)
connection = pika.BlockingConnection(pika.ConnectionParameters(**params))
channel = connection.channel()
