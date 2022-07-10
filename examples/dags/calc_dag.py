from ldag.dag import DAG
from ldag.communication import pull, push
from ldag.tasks import PythonTask


def sum_two_numbers(dag: DAG):
    a = dag.params.get("a")
    b = dag.params.get("b")
    s = a + b
    push(dag, "task_1_sum", s)


def sum_saved_number_with_number_from_param(dag: DAG):
    saved_number = pull(dag, "task_1_sum")
    c = dag.params.get("c")
    s = saved_number + c
    push(dag, "task_2_sum", s)


def print_sum(dag: DAG):
    calculated_sum = pull(dag, "task_2_sum")
    print(calculated_sum)


sum_dag = DAG(name="SimpleSumDag", params={})

with sum_dag:
    sum_task_1 = PythonTask(
        dag=sum_dag, task_id="sum_task_1", func=sum_two_numbers
    )
    sum_task_2 = PythonTask(
        dag=sum_dag,
        task_id="sum_task_2",
        func=sum_saved_number_with_number_from_param,
    )
    print_task = PythonTask(
        dag=sum_dag, task_id="print_task", func=print_sum
    )

    sum_task_1 >> sum_task_2
    sum_task_2 >> print_task

    sum_dag.set_entry_task(sum_task_1)
