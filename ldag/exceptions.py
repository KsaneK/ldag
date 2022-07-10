class BaseTaskError(Exception):
    def __init__(self, task, dag):
        super(BaseTaskError, self).__init__()
        self._task = task
        self._dag = dag


class TaskIdAlreadyRegisteredInDAGError(BaseTaskError):
    def __str__(self):
        return f"Task {self._task.task_id} is already registered in dag {self._dag.name}"


class CycleInDAGDetectedError(BaseTaskError):
    def __str__(self):
        return (
            f"Cycle detected in dag {self._dag.name}. "
            f"Task \"{self._task.task_id}\" visited multiple times"
        )


class TaskNotConnectedError(BaseTaskError):
    def __str__(self):
        return f"Task {self._task.task_id} not connected to anything in dag {self._dag.name}"
