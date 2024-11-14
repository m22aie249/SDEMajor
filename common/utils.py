import os
from common.constants import DB_NAME, DB_USER, DB_PASSWORD, PORT_NUMBER

def get_file_name(path: str) -> str:
    return path.split('/')[-1].split('.')[0]


def get_neo4j_url() -> str:
    return f"bolt://localhost:{PORT_NUMBER}"


def get_env_var_value(env: str, default:str = None) -> str:
    if default is not None and default.strip() != "":
        return os.environ.get(env, default=default)
    else:
        return os.environ.get(env)


def get_db_name(x:str=None) -> str:
    if x is not None and x != "":
        return get_env_var_value(DB_NAME, x)
    else:
        return get_env_var_value(DB_NAME)


def get_db_user(x:str=None) -> str:
    if x is not None and x != "":
        return get_env_var_value(DB_USER, x)
    else:
        return get_env_var_value(DB_USER)


def get_db_password(x:str=None) -> str:
    if x is not None and x != "":
        return get_env_var_value(DB_PASSWORD, x)
    else:
        return get_env_var_value(DB_PASSWORD)