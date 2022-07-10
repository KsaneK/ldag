from ldag.trigger import trigger_dag

dag_id = "calc_dag.sum_dag"
dag_params = {
    "a": 1,
    "b": 2,
    "c": 3,
}

trigger_dag(dag_id=dag_id, dag_params=dag_params)
