import importlib
import os

from ldag.config import dags_directory


file_name_to_module = {}
for dag_file_name in os.listdir(dags_directory):
    if dag_file_name == "__init__.py" or not dag_file_name.endswith(".py"):
        continue

    dag_module_name = dag_file_name[:-3]
    file_name_to_module[dag_module_name] = (
        importlib.import_module(f"{dags_directory}.{dag_module_name}")
    )
