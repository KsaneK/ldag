from typing import Callable

from ldag.tasks import BaseTask


class PythonTask(BaseTask):
    def __init__(self, func: Callable, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._func = func

    def execute(self):
        self._func()
