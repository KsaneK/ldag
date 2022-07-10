from typing import Any, Dict

from ldag.task import AbstractTask


class DAG:
    def __init__(self, name: str, params: Dict[str, Any]):
        self._name = name
        self._params = params
        self._first_task = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> Dict[str, Any]:
        return self._params

    def set_first_task(self, task: AbstractTask):
        self._first_task = task

    def trigger(self):
        ...
