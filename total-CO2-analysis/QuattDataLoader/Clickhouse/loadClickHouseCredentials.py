import os
from dotenv import load_dotenv

desktop_path = os.path.join(os.path.expanduser("~"), ".clickhouse_env")
load_dotenv(desktop_path)

clickhouse_host     = os.getenv("CH_HOST")
clickhouse_user     = os.getenv("CH_USER")
clickhouse_password = os.getenv("CH_PASSWORD")
clickhouse_port     = os.getenv("CH_PORT")












