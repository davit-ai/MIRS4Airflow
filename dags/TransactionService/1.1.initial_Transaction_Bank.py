import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import pyodbc
import psycopg2
import csv
from io import StringIO
import logging

local_tz = pendulum.timezone("Asia/Kuala_Lumpur")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 26),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    '1.01.initial_bank',
    default_args=default_args,
    description='Transfer data from mssql.dbo.mirs to postgres.public.report',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Initial Data Load', 'MIRS4_BANK']
)


def get_bank_columns(dest_cur):
    dest_cur.execute("""
        SELECT column_name
        FROM report_table_list
        WHERE table_name = 'TransactionService_Bank'
        ORDER BY order_position
    """)
    columns = dest_cur.fetchall()
    column_names = [f'"{col[0]}"' for col in columns]
    logging.info(f"Destination columns: {column_names}")
    return column_names


def transfer_data():
    try:
        source_conn_info = BaseHook.get_connection('TransactionDB_MSSQL')
        dest_conn_info = BaseHook.get_connection('ReportDB_Postgres')

        source_conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={source_conn_info.host},{source_conn_info.port};"
            f"DATABASE={source_conn_info.schema};"
            f"UID={source_conn_info.login};"
            f"PWD={source_conn_info.password};"
            "TrustServerCertificate=yes;")
        source_conn = pyodbc.connect(source_conn_str)
        logging.info("Connected to source database.")

        dest_conn_str = f"dbname={dest_conn_info.schema} user={dest_conn_info.login} password={dest_conn_info.password} host={dest_conn_info.host} port={dest_conn_info.port}"
        dest_conn = psycopg2.connect(dest_conn_str)
        logging.info("Connected to destination database.")

        with source_conn.cursor() as source_cur, dest_conn.cursor() as dest_cur:
            columns = get_bank_columns(dest_cur)
            column_str = ', '.join(columns)

            # Truncate the table only once before starting the batch inserts
            truncate_query = 'TRUNCATE TABLE report.public."TransactionService_Bank"'
            dest_cur.execute(truncate_query)
            logging.info("Destination table truncated.")

            batch_size = 100
            offset = 0

            while True:
                query = f"""
                    EXEC dbo.usp_bank_pipeline @Offset={offset}, @FetchNext={batch_size}
                """

                source_cur.execute(query)
                rows = source_cur.fetchall()
                if not rows:
                    break

                buffer = StringIO()
                writer = csv.writer(buffer, delimiter='\t')
                writer.writerows(rows)
                buffer.seek(0)

                # Perform bulk insert using COPY
                dest_cur.copy_expert(
                    f"""
                    COPY report.public."TransactionService_Bank" ({column_str})
                    FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')
                    """, buffer)
                dest_conn.commit()
                logging.info(f"Batchs inserted into Bank table. Rows inserted: {len(rows)}")

                offset += batch_size

            # Update sync details once all data has been inserted
            current_timestamp = pendulum.now("Asia/Kuala_Lumpur")
            dest_cur.execute("""
                UPDATE report.public.data_sync_details
                SET last_sync_date = %s AT TIME ZONE 'Asia/Kuala_Lumpur'
                WHERE table_name = 'TransactionService_Bank'
                """, (current_timestamp,))
            dest_conn.commit()
            logging.info("Data transfer completed successfully and sync details updated.")

    except Exception as e:
        logging.error("Error occurred while transferring data: %s", e)
        raise


transfer_data_task = PythonOperator(
    task_id='1.01.initial_bank',
    python_callable=transfer_data,
    dag=dag
)