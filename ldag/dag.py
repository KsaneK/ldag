from typing import Any, Dict

from ldag.tasks import AbstractTask


class DAG:
    def __init__(self, name: str, params: Dict[str, Any]):
        self._name = name
        self._params = params
        self._entry_task = None
        self._registered_tasks = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> Dict[str, Any]:
        return self._params

    def set_entry_task(self, task: AbstractTask):
        self._entry_task = task

    def register_task(self, task: AbstractTask):
        self._registered_tasks.append(task)

    def trigger(self):
        ...
