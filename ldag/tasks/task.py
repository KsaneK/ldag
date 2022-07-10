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

    @property
    @abstractmethod
    def task_id(self):
        """Returns task id"""
        ...

    @abstractmethod
    def add_downstream(self, task):
        """Sets downstream task"""
        ...

    @abstractmethod
    def add_upstream(self, task):
        """Sets upstream task"""
        ...

    @property
    @abstractmethod
    def downstream_tasks(self):
        """Returns downstream task"""
        ...

    @property
    @abstractmethod
    def upstream_tasks(self):
        """Returns upstream task"""
        ...
