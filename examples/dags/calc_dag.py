from ldag.dag import DAG
from ldag.communication import pull, push
from ldag.tasks import PythonTask


def sum_two_numbers(dag: DAG, a: int, b: int):
    s = a + b
    push(dag, "task_1_sum", s)


def sum_saved_number_with_number_from_param(dag: DAG, c: int):
    saved_number = pull(dag, "task_1_sum")
    s = saved_number + c
    push(dag, "task_2_sum", s)


def print_sum(dag: DAG):
    calculated_sum = pull(dag, "task_2_sum")
    print(calculated_sum)


dag = DAG(name="SimpleSumDag", params={})

sum_task_1 = PythonTask(
    dag=dag, task_id="sum_task_1", func=sum_two_numbers, params={"a": 1, "b": 2}
)
sum_task_2 = PythonTask(
    dag=dag,
    task_id="sum_task_2",
    func=sum_saved_number_with_number_from_param,
    params={"c": 3}
)
print_task = PythonTask(
    dag=dag, task_id="print_task", func=print_sum, params={}
)

sum_task_1 >> sum_task_2
sum_task_2 >> print_task

dag.set_entry_task(sum_task_1)