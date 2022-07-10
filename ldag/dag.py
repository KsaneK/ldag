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
        queue = [self._entry_task]
        visited_edges = set()
        visited_tasks = set()
        to_be_connected = set()

        while queue:
            task = queue.pop(0)
            if task in visited_tasks:
                raise CycleInDAGDetectedError(task, self)
            visited_tasks.add(task)

            for downstream_task in task.downstream_tasks:
                visited_edges.add((task, downstream_task))
                if all(
                    (upstream_task, downstream_task) in visited_edges
                    for upstream_task in downstream_task.upstream_tasks
                ):
                    queue.append(downstream_task)
                    if downstream_task in to_be_connected:
                        to_be_connected.remove(downstream_task)
                else:
                    to_be_connected.add(downstream_task)

        for task in self._registered_tasks:
            if task in to_be_connected:
                raise CycleInDAGDetectedError(task, self)
            elif task not in visited_tasks:
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
