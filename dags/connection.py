import pyodbc
import psycopg2
import logging
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook


def get_Transaction_connection():
    source_conn = BaseHook.get_connection('TransactionDB_MSSQL')
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={source_conn.host};"
        f"DATABASE={source_conn.schema};"
        f"UID={source_conn.login};"
        f"PWD={source_conn.password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def get_Report_connection():
    dest_conn_info = BaseHook.get_connection('ReportDB_Postgres')
    conn_str = (
    f"dbname={dest_conn_info.schema} user={dest_conn_info.login} password={dest_conn_info.password} host={dest_conn_info.host} port={dest_conn_info.port}"
    )
    return psycopg2.connect(conn_str)

def close_sql_server_connection(conn):
    try:
        conn.close()
    except Exception as e:
        print(f"Error closing SQL Server connection: {e}")
