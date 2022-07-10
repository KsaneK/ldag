import json
from typing import Any, Dict

from ldag.constants import DAG_ID, DAG_PARAMS
from ldag.dag import DAG
from ldag.rabbitmq import channel, tasks_queue_name


def trigger_dag(dag_id: str, dag_params: Dict[str, Any]) -> None:
    payload = {
        DAG_ID: dag_id,
        DAG_PARAMS: dag_params,
    }

    channel.queue_declare(tasks_queue_name, durable=True)
    channel.basic_publish(exchange="", routing_key=tasks_queue_name, body=json.dumps(payload))
