import copy
import importlib
import json

from ldag import DAG
from ldag.constants import DAG_ID, DAG_PARAMS, RUN_ID, STATUS, TASK_ID
from ldag.config import dags_directory
from ldag.rabbitmq import channel, dag_trigger_queue_name, tasks_queue_name, tasks_status_queue_name
from ldag.tasks import AbstractTask
from ldag.tasks.task import TaskStatus


def dag_trigger_callback(ch, method, properties, body):
    print("<- DAG trigger request", end="")
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
    dag_object._prepare_task_mapping()
    dag_object.trigger()
    run_id_to_triggered_dag[dag_object.run_id] = dag_object
    print(f" - Triggering {dag_object}, dag_id: {dag_id}, run_id: {dag_object.run_id}")
    queue_task(dag=dag_object, dag_id=dag_id, task=dag_object.entry_task)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def task_status_callback(ch, method, properties, body):
    print("<- Task status received", end="")
    payload = json.loads(body)
    dag_id = payload[DAG_ID]
    run_id = payload[RUN_ID]
    task_id = payload[TASK_ID]
    status = payload[STATUS]

    dag_object: DAG = run_id_to_triggered_dag[run_id]
    task: AbstractTask = dag_object._task_mapping[task_id]
    task.set_status(TaskStatus(status))
    print(f" - run_id={run_id}, task_id={task_id}, status={status}")
    for downstream_task in task.downstream_tasks:
        if all(
            upstream_task.status == TaskStatus.SUCCEEDED
            for upstream_task in downstream_task.upstream_tasks
        ):
            queue_task(dag=dag_object, dag_id=dag_id, task=downstream_task)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def queue_task(dag: DAG, dag_id: str, task: AbstractTask):
    print(f"-> Queueing task {dag.name}.{task.task_id}")
    payload = {
        DAG_ID: dag_id,
        DAG_PARAMS: dag.params,
        RUN_ID: dag.run_id,
        TASK_ID: task.task_id,
    }
    channel.basic_publish(exchange="", routing_key=tasks_queue_name, body=json.dumps(payload))


run_id_to_triggered_dag = {}
channel.queue_declare(queue=dag_trigger_queue_name, durable=True)
channel.queue_declare(queue=tasks_queue_name, durable=True)
channel.queue_declare(queue=tasks_status_queue_name, durable=True)
channel.basic_consume(dag_trigger_queue_name, on_message_callback=dag_trigger_callback)
channel.basic_consume(tasks_status_queue_name, on_message_callback=task_status_callback)
print("[*] Waiting for DAGs")
channel.start_consuming()
