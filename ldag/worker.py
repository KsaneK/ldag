import copy
import importlib
import json

from ldag import DAG
from ldag.constants import DAG_ID, DAG_PARAMS, RUN_ID, TASK_ID, STATUS
from ldag.config import dags_directory
from ldag.rabbitmq import channel, tasks_queue_name, tasks_status_queue_name
from ldag.tasks.task import TaskStatus


def new_task_callback(ch, method, properties, body):
    print("[*] New task", end="")
    parsed_body = json.loads(body)
    dag_id = parsed_body[DAG_ID]
    module_name, dag_object_name = dag_id.split(".")
    dag_params = parsed_body[DAG_PARAMS]
    run_id = parsed_body[RUN_ID]
    task_id = parsed_body[TASK_ID]

    module = (
        importlib.import_module(f"{dags_directory}.{module_name}")
    )
    dag_object: DAG = getattr(module, dag_object_name)
    dag_object = copy.deepcopy(dag_object)
    dag_object.update_params(dag_params)
    dag_object._prepare_task_mapping()

    task_to_execute = dag_object._task_mapping[task_id]
    try:
        print(f" - Executing task {task_to_execute.task_id}")
        task_to_execute.execute()
        status = TaskStatus.SUCCEEDED
    except Exception as e:
        print(e)
        status = TaskStatus.FAILED
    update_task_status(dag_id=dag_id, run_id=run_id, task_id=task_id, status=status)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def update_task_status(run_id: str, task_id: str, dag_id: str, status: TaskStatus):
    payload = {
        DAG_ID: dag_id,
        RUN_ID: run_id,
        TASK_ID: task_id,
        STATUS: status.value,
    }
    channel.basic_publish(
        exchange="", routing_key=tasks_status_queue_name, body=json.dumps(payload)
    )


run_id_to_triggered_dag = {}
channel.queue_declare(queue=tasks_queue_name, durable=True)
channel.queue_declare(queue=tasks_status_queue_name, durable=True)
channel.basic_consume(tasks_queue_name, on_message_callback=new_task_callback)
print("[*] Waiting for tasks")
channel.start_consuming()
