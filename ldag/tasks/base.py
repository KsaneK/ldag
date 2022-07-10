from abc import abstractmethod
from typing import Optional, Union, Iterable, List

from ldag.tasks.task import AbstractTask


class BaseTask(AbstractTask):
    def __init__(self, dag, task_id: str):
        self._task_id = task_id
        self._dag = dag
        self._downstream_tasks: List[AbstractTask] = []
        self._upstream_tasks: List[AbstractTask] = []
        self._register_in_dag()

    def add_downstream(self, task: Union[AbstractTask, Iterable[AbstractTask]]):
        if isinstance(task, AbstractTask):
            self._downstream_tasks.append(task)
        elif isinstance(task, Iterable):
            self._downstream_tasks.extend(task)

    def add_upstream(self, task: Union[AbstractTask, Iterable[AbstractTask]]):
        if isinstance(task, AbstractTask):
            self._upstream_tasks.append(task)
        elif isinstance(task, Iterable):
            self._upstream_tasks.extend(task)

    @property
    def dag(self):
        return self._dag

    @property
    def task_id(self):
        return self._task_id

    @property
    def downstream_tasks(self) -> Optional[Union[AbstractTask, Iterable[AbstractTask]]]:
        return self._downstream_tasks

    @property
    def upstream_tasks(self) -> Optional[Union[AbstractTask, Iterable[AbstractTask]]]:
        return self._upstream_tasks

    @abstractmethod
    def execute(self):
        ...

    def __lshift__(self, other):
        self.add_upstream(other)

    def __rshift__(self, other):
        self.add_downstream(other)

    def __str__(self):
        return f"{self.__class__.__name__}-{self._task_id}"

    def _register_in_dag(self):
        self._dag.register_task(self)


class DummyTask(BaseTask):
    def execute(self):
        pass
