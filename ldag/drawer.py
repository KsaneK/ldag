from functools import partial
from typing import Tuple, List, Dict, Any

import matplotlib.pyplot as plt
import networkx as nx

from ldag import DAG
from ldag.tasks import AbstractTask


def draw_dag(dag: DAG):
    g = nx.MultiDiGraph()
    edges = create_edges(dag.entry_task)
    g.add_edges_from(edges)
    plt.figure(f"DAG: {dag.name}")
    nx.draw(g, connectionstyle="arc3, rad = 0.1", with_labels=True)
    plt.show()


def create_edges(entry_task: AbstractTask) -> List[Tuple[str, str]]:
    queue = [entry_task]
    visited = set()
    edges = []

    while queue:
        current_task: AbstractTask = queue.pop(0)
        if current_task in visited:
            continue
        visited.add(current_task)

        for downstream_task in current_task.downstream_tasks:
            queue.append(downstream_task)
            edges.append((current_task.task_id, downstream_task.task_id))

    return edges
