from typing import Any, Dict, Set, Optional

from ldag.exceptions import TaskIdAlreadyRegisteredInDAGError, CycleInDAGDetectedError, \
    TaskNotConnectedError
from ldag.tasks import AbstractTask


class DAG:
    def __init__(self, name: str, params: Dict[str, Any]):
        self._name = name
        self._params = params
        self._entry_task: Optional[AbstractTask] = None
        self._registered_tasks: Set[AbstractTask] = set()
        self._task_mapping: Dict[str, AbstractTask] = dict()

    @property
    def name(self) -> str:
        return self._name

    @property
    def params(self) -> Dict[str, Any]:
        return self._params

    @property
    def entry_task(self) -> Optional[AbstractTask]:
        return self._entry_task

    def update_params(self, params: Dict[str, Any]) -> None:
        self._params.update(params)

    def set_entry_task(self, task: AbstractTask):
        self._entry_task = task

    def register_task(self, task: AbstractTask):
        if task in self._registered_tasks:
            raise TaskIdAlreadyRegisteredInDAGError(task=task, dag=self)
        self._registered_tasks.add(task)

    def trigger(self):
        ...

    def _validate(self):
        task_to_visited_mapping = {task.task_id: False for task in self._registered_tasks}
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

    def _prepare_task_mapping(self):
        for task in self._registered_tasks:
            self._task_mapping[task.task_id] = task

    def __str__(self):
        return f"DAG(name={self._name}, params={self._params})"

    def __enter__(self):
        ...

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._validate()
