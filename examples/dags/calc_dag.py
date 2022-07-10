from ldag.dag import DAG
from ldag.communication import pull, push
from ldag.tasks import PythonTask
from ldag.tasks.base import DummyTask


def sum_two_numbers(dag: DAG, number1_key: str, number2_key: str, output_key: str):
    a = dag.params.get(number1_key)
    b = dag.params.get(number2_key)
    s = a + b
    print(f"Task(sum_two_numbers): {a} + {b} = {s}")
    push(dag, output_key, s)


def sum_saved_numbers(dag: DAG, number1_key: str, number2_key: str, output_key: str):
    saved_number_1 = pull(dag, number1_key)
    saved_number_2 = pull(dag, number2_key)
    s = saved_number_1 + saved_number_2
    print(f"Task(sum_saved_numbers): {saved_number_1} + {saved_number_2} = {s}")
    push(dag, output_key, s)


def print_sum(dag: DAG, result_key: str):
    calculated_sum = pull(dag, result_key)
    print(calculated_sum)


sum_dag = DAG(name="SimpleSumDag", params={})

with sum_dag:
    start_task = DummyTask(dag=sum_dag, task_id="start")
    sum_task_1 = PythonTask(
        dag=sum_dag,
        task_id="sum_task_1",
        func=sum_two_numbers,
        params={"number1_key": "a", "number2_key": "b", "output_key": "task_1_sum"}
    )
    sum_task_2 = PythonTask(
        dag=sum_dag,
        task_id="sum_task_2",
        func=sum_two_numbers,
        params={"number1_key": "c", "number2_key": "d", "output_key": "task_2_sum"}
    )
    sum_task_3 = PythonTask(
        dag=sum_dag,
        task_id="sum_task_3",
        func=sum_two_numbers,
        params={
            "number1_key": "task_1_sum", "number2_key": "task_2_sum", "output_key": "task_3_sum"
        }
    )
    print_task = PythonTask(
        dag=sum_dag, task_id="print_task", func=print_sum, params={"result_key": "task_3_sum"}
    )

    start_task >> [sum_task_1, sum_task_2] >> sum_task_3 >> print_task

    sum_dag.set_entry_task(start_task)
