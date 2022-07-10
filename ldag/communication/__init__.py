import json
from typing import Any, Type, Union, Dict, List

from ldag.rabbitmq import channel


def pull(
        run_id: str, key: str
) -> Union[Dict[str, Any], List[Any], int, str, float, bool, Type[None]]:
    queue = f"{run_id}-{key}"
    channel.queue_declare(queue, auto_delete=True)
    print(f"Pulling data from queue {queue}")
    method_frame, header_frame, body = channel.basic_get(queue=queue)

    if method_frame.NAME == "Basic.GetEmpty":
        return None

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
    data = json.loads(body)

    return data


def push(
        run_id: str,
        key: str,
        value: Union[Dict[str, Any], List[Any], int, str, float, bool, Type[None]]
) -> None:
    queue = f"{run_id}-{key}"
    data = json.dumps(value)
    channel.queue_declare(queue, auto_delete=True)
    print(f"Pushing data {data} to queue {queue}")
    channel.basic_publish(exchange="", routing_key=queue, body=data)
