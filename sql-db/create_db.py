from common.constants import DB_NAME, DB_USER, DB_PASSWORD
from common.utils import *

import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

db_name = os.environ.get(DB_NAME)
db_user = os.environ.get(DB_USER)
db_pwd = os.environ.get(DB_PASSWORD)

mydb = mysql.connector.connect(
  host="localhost",
  user=db_user,
  password=db_pwd,
)

my_cursor = mydb.cursor()

try:
    my_cursor.execute(f"DROP DATABASE {db_name}")
except:
    print("No need to delete any database as it doesn't exists")

my_cursor.execute(f"CREATE DATABASE {db_name}")

