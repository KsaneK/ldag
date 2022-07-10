from abc import abstractmethod
from typing import Optional, Union, Iterable, List

from ldag.tasks.task import AbstractTask, TaskStatus


class BaseTask(AbstractTask):
    def __init__(self, dag, task_id: str):
        self._task_id = task_id
        self._dag = dag
        self._status: TaskStatus = TaskStatus.QUEUED
        self._downstream_tasks: List["BaseTask"] = []
        self._upstream_tasks: List["BaseTask"] = []
        self._register_in_dag()

    def add_downstream(self, task: Union["BaseTask", Iterable["BaseTask"]]):
        if isinstance(task, BaseTask):
            self._downstream_tasks.append(task)
            task._upstream_tasks.append(self)
        elif isinstance(task, Iterable):
            self._downstream_tasks.extend(task)
            for downstream_task in task:
                downstream_task._upstream_tasks.append(self)

    def add_upstream(self, task: Union["BaseTask", Iterable["BaseTask"]]):
        if isinstance(task, BaseTask):
            self._upstream_tasks.append(task)
            task._downstream_tasks.append(self)
        elif isinstance(task, Iterable):
            self._upstream_tasks.extend(task)
            for upstream_task in task:
                upstream_task._downstream_tasks.append(self)

    def set_status(self, status: TaskStatus):
        self._status = status

    @property
    def dag(self):
        return self._dag

    @property
    def task_id(self):
        return self._task_id

    @property
    def status(self) -> TaskStatus:
        return self._status

    @property
    def downstream_tasks(self) -> Optional[Union["BaseTask", Iterable["BaseTask"]]]:
        return self._downstream_tasks

    @property
    def upstream_tasks(self) -> Optional[Union["BaseTask", Iterable["BaseTask"]]]:
        return self._upstream_tasks

    @abstractmethod
    def execute(self):
        ...

    def __lshift__(self, other):
        self.add_upstream(other)
        return other

    def __rshift__(self, other):
        self.add_downstream(other)
        return other

    def __rlshift__(self, other):
        self.add_downstream(other)
        return self

    def __rrshift__(self, other):
        self.add_upstream(other)
        return self

    def __str__(self):
        return f"{self.__class__.__name__}-{self._task_id}"

    def _register_in_dag(self):
        self._dag.register_task(self)


class DummyTask(BaseTask):
    def execute(self):
        pass
