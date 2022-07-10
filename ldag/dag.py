from typing import Any, Dict

from ldag.exceptions import TaskAlreadyRegisteredInDAGError, CycleInDAGDetectedError, \
    TaskNotConnectedError
from ldag.tasks import AbstractTask


class DAG:
    def __init__(self, name: str, params: Dict[str, Any]):
        self._name = name
        self._params = params
        self._entry_task = None
        self._registered_tasks = set()

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> Dict[str, Any]:
        return self._params

    def update_params(self, params: Dict[str, Any]) -> None:
        self._params.update(params)

    def set_entry_task(self, task: AbstractTask):
        self._entry_task = task

    def register_task(self, task: AbstractTask):
        print("registering task", task)
        if task in self._registered_tasks:
            raise TaskAlreadyRegisteredInDAGError(task=task, dag=self)
        self._registered_tasks.add(task)

    def trigger(self):
        ...

    def _validate(self):
        task_to_visited_mapping = {str(task): False for task in self._registered_tasks}
        queue = []
        current_task = self._entry_task
        queue.append(current_task)
        task_to_visited_mapping[str(current_task)] = True

        while queue:
            current_task: AbstractTask = queue.pop(0)

            for downstream_task in current_task.downstream_tasks:
                if task_to_visited_mapping[str(downstream_task)] is True:
                    raise CycleInDAGDetectedError(downstream_task, self)
                queue.append(downstream_task)
                task_to_visited_mapping[str(downstream_task)] = True

        for task, visited in task_to_visited_mapping.items():
            if not visited:
                raise TaskNotConnectedError(task, self)

    def __str__(self):
        return f"DAG(name={self._name}, params={self._params})"

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._validate()
