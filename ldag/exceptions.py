class BaseTaskError(Exception):
    def __init__(self, task, dag):
        super(BaseTaskError, self).__init__()
        self._task = task
        self._dag = dag


class TaskAlreadyRegisteredInDAGError(BaseTaskError):
    def __str__(self):
        return f"Task {self._task._task_id} is already registered in dag {self._dag.name}"


class CycleInDAGDetectedError(BaseTaskError):
    def __str__(self):
        return f"Cycle detected in dag {self._dag.name}. Task {self._task} visited multiple times"


class TaskNotConnectedError(BaseTaskError):
    def __str__(self):
        return f"Task {self._task} not connected to anything in dag {self._dag.name}"
