import copy
import importlib
import json

from ldag import DAG
from ldag.constants import DAG_ID, DAG_PARAMS
from ldag.config import dags_directory
from ldag.rabbitmq import channel, tasks_queue_name


def callback(ch, method, properties, body):
    print("[*] New task", end="")
    parsed_body = json.loads(body)
    dag_id = parsed_body[DAG_ID]
    module_name, dag_object_name = dag_id.split(".")
    dag_params = parsed_body[DAG_PARAMS]

    module = (
        importlib.import_module(f"{dags_directory}.{module_name}")
    )
    dag_object: DAG = getattr(module, dag_object_name)
    dag_object = copy.deepcopy(dag_object)
    dag_object.update_params(dag_params)
    print(f" - Triggering {dag_object}, dag_id: {dag_id}")
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.queue_declare(queue=tasks_queue_name, durable=True)
channel.basic_consume(tasks_queue_name, on_message_callback=callback)
print("[*] Waiting for tasks")
channel.start_consuming()
