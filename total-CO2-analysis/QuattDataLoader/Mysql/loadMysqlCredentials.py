import os
from dotenv import load_dotenv

desktop_path = os.path.join(os.path.expanduser("~"), ".mysql_env")
load_dotenv(desktop_path)
mysql_host     = os.getenv("MYSQL_HOST")
mysql_user     = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_port     = os.getenv("MYSQL_PORT")
mysql_database = os.getenv("MYSQL_DATABASE")







