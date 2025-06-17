import psycopg2
from config import *
import snowflake.connector


def get_redshift_connection():
    conn = psycopg2.connect(**red_shift_conn_params)
    print("**************************** red shift connection established ****************************")
    return conn


def get_snowflake_connection():
    conn = snowflake.connector.connect(**conn_params)

    print(" **************************** snowflake connection established ****************************")
    return conn