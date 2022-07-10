import configparser

__all__ = ["app_id", "dags_directory"]

config = configparser.ConfigParser()
config.read("ldag_config")
app_id = config.get("default", "APP_ID")
dags_directory = config.get("default", "DAGS_DIRECTORY")
dags_directory = dags_directory.replace("/", ".")
if dags_directory.endswith("."):
    dags_directory = dags_directory[:-1]
