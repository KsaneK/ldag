from abc import abstractmethod
from typing import Optional, Union, Iterable

from ldag.tasks.task import AbstractTask


class BaseTask(AbstractTask):
    def __init__(self, dag, task_id: str):
        self._task_id = task_id
        self._dag = dag
        self._downstream_task: Optional[Union[AbstractTask, Iterable[AbstractTask]]] = None
        self._upstream_task: Optional[Union[AbstractTask, Iterable[AbstractTask]]] = None
        self._register_in_dag()

    def set_downstream(self, task: Optional[Union[AbstractTask, Iterable[AbstractTask]]]):
        self._downstream_task = task

    def set_upstream(self, task: Optional[Union[AbstractTask, Iterable[AbstractTask]]]):
        self._upstream_task = task

    @property
    def dag(self):
        return self._dag

    @property
    def downstream_task(self) -> Optional[Union[AbstractTask, Iterable[AbstractTask]]]:
        return self._downstream_task

    @property
    def upstream_task(self) -> Optional[Union[AbstractTask, Iterable[AbstractTask]]]:
        return self._upstream_task

    @abstractmethod
    def execute(self):
        ...

    def __lshift__(self, other):
        self.set_upstream(other)

    def __rshift__(self, other):
        self.set_downstream(other)

    def __str__(self):
        return f"{self.__class__.__name__}-{self._task_id}"

    def _register_in_dag(self):
        self._dag.register_task(self)
