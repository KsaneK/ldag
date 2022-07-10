from typing import Any, Type, Union, Dict, List

from ldag import DAG


def pull(
        dag: DAG, key: str
) -> Union[Dict[str, Any], List[Any], int, str, float, bool, Type[None]]:
    ...


def push(
        dag: DAG,
        key: str,
        value: Union[Dict[str, Any], List[Any], int, str, float, bool, Type[None]]
) -> None:
    ...
