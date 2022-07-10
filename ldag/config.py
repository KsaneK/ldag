import configparser

__all__ = [
    "app_id",
    "dags_directory",
    "rabbitmq_host",
    "rabbitmq_port",
    "rabbitmq_user",
    "rabbitmq_password"
]

config = configparser.ConfigParser(allow_no_value=True)
config.read("ldag_config")
app_id = config.get("default", "APP_ID")
dags_directory = config.get("default", "DAGS_DIRECTORY")
dags_directory = dags_directory.replace("/", ".")
if dags_directory.endswith("."):
    dags_directory = dags_directory[:-1]

rabbitmq_host = config.get("rabbitmq", "host")
rabbitmq_port = config.getint("rabbitmq", "port")
rabbitmq_user = config.get("rabbitmq", "user")
rabbitmq_password = config.get("rabbitmq", "password")

if rabbitmq_user == "":
    rabbitmq_user = None

if rabbitmq_password == "":
    rabbitmq_password = None
