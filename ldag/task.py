from abc import ABC, abstractmethod
from typing import Union, Iterable, Optional


class AbstractTask(ABC):
    @abstractmethod
    def execute(self):
        ...

    @abstractmethod
    def set_downstream(self, task):
        ...

    @abstractmethod
    def set_upstream(self, task):
        ...

    @property
    @abstractmethod
    def downstream_task(self):
        ...

    @property
    @abstractmethod
    def upstream_task(self):
        ...


class BaseTask(AbstractTask):
    def __init__(self, task_id: str):
        self._task_id = task_id
        self._downstream_task: Optional[Union[AbstractTask, Iterable[AbstractTask]]] = None
        self._upstream_task: Optional[Union[AbstractTask, Iterable[AbstractTask]]] = None

    def set_downstream(self, task: Optional[Union[AbstractTask, Iterable[AbstractTask]]]):
        self._downstream_task = task

    def set_upstream(self, task: Optional[Union[AbstractTask, Iterable[AbstractTask]]]):
        self._upstream_task = task

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
