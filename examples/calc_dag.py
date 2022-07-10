from ldag import DAG
from ldag.communication import pull, push
from ldag.tasks import PythonTask


def sum_two_numbers(dag: DAG, a: int, b: int):
    s = a + b
    push(dag, "sum_of_task_1", s)


sum_dag = DAG(name="SimpleSumDag", params={})

sum_task_1 = PythonTask(task_id="sum_task_1", func=lambda a, b: a + b, params={"a": 1, "b": 2})
sum_task_2 = PythonTask(task_id="sum_task_2", func=lambda a, b: a + b, params={"c": 3})

sum_task_1 >> sum_task_2

sum_dag.set_first_task(sum_task_1)
