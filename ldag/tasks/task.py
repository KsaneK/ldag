from abc import ABC, abstractmethod


class AbstractTask(ABC):
    @abstractmethod
    def execute(self):
        """Executes task"""
        ...

    @abstractmethod
    def dag(self):
        """Returns DAG associated to task"""
        ...

    @abstractmethod
    def set_downstream(self, task):
        """Sets downstream task"""
        ...

    @abstractmethod
    def set_upstream(self, task):
        """Sets upstream task"""
        ...

    @property
    @abstractmethod
    def downstream_task(self):
        """Returns downstream task"""
        ...

    @property
    @abstractmethod
    def upstream_task(self):
        """Returns upstream task"""
        ...
