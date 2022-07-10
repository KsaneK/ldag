from typing import Callable, Any, Dict

from ldag.tasks import BaseTask


class PythonTask(BaseTask):
    def __init__(self, func: Callable, params: Dict[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._func = func
        self._params = params

    def execute(self):
        self._func(dag=self._dag, **self._params)
