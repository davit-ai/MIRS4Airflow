import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
from io import StringIO
from connection import get_Report_connection, get_Transaction_connection
from constants import SP_TRAN_TransactionStatusHistory_CONFIGURATION
from common_args import default_args

local_tz = pendulum.timezone("Asia/Kuala_Lumpur")


dag = DAG(
    '1.6.0.trans_TransactionStatusHistorysdata',
    default_args=default_args,
    description='Transfer TransactionStatusHistorys data',
    schedule_interval=None,
    catchup=False,
    tags=['TransactionStatusHistory']
)
formatted_timestamp = pendulum.now().format('YYYY-MM-DD HH:mm:ss')
def get_db_connection():
    source_conn = get_Transaction_connection()
    source_cursor = source_conn.cursor()
    print("source_conn: connected to source db")
    dest_conn = get_Report_connection()
    dest_cursor = dest_conn.cursor()
    print("dest_conn: connected to destination db")
    return dest_conn,dest_cursor,source_conn,source_cursor

def get_last_sync_date():
    dest_conn, dest_cursor, source_conn, source_cursor = get_db_connection()
    dest_conn.execute("""
        SELECT last_sync_date
        FROM data_sync_details
        WHERE sync_enable = true AND table_name = 'TransactionService_TransactionStatusHistory'
    """)
    last_sync_date = dest_cursor.fetchone()[0]
    logging.info(f"Fetched last_sync_date: {last_sync_date}")
    return last_sync_date

def transfer_data():
    try:
        dest_conn, dest_cursor, source_conn, source_cursor = get_db_connection()
        batch_size = 100
        offset = 0
        # Fetch data
        source_cursor.execute(f"EXEC {SP_TRAN_TransactionStatusHistory_CONFIGURATION} @Offset = {offset}, @FetchNext = {batch_size}")
       ## source_cursor.execute(f"EXEC {SP_TRAN_TransactionStatusHistory_CONFIGURATION}")
        rows = source_cursor.fetchall()

        buffer = StringIO()
        writer = csv.writer(buffer, delimiter='\t')
        writer.writerows(rows)
        buffer.seek(0)

        # get table list from destination db
        dest_cursor.execute("""
        SELECT column_name
        FROM report_table_list
        WHERE table_name = 'TransactionService_TransactionStatusHistory'
        ORDER BY order_position
        """)
        columns = [f'"{row[0]}"' for row in dest_cursor.fetchall()]
        column_str = ', '.join(columns)
        print(column_str)

        for row in rows:
                # Check for existing ID in destination
                existing_id_query = 'SELECT "TransactionId" FROM "TransactionService_TransactionStatusHistory" WHERE "TransactionId" = %s'
                dest_cursor.execute(existing_id_query, (row[3],))
                existing_id = dest_cursor.fetchone()

                if existing_id:
                    # Delete existing row before inserting new data
                    delete_query = 'DELETE FROM "TransactionService_TransactionStatusHistory" WHERE "TransactionId" = %s'
                    logging.info(f"Deleting existing row for userid: {row[0]}")
                    dest_cursor.execute(delete_query, (row[3],))

                # Insert new row
                insert_query = f"""
                    INSERT INTO "TransactionService_TransactionStatusHistory" ({column_str})
                    VALUES ({', '.join(['%s'] * len(columns))})
                """
                logging.info(f"Executing INSERT query with values {row}")
                dest_cursor.execute(insert_query, row)
                dest_conn.commit()
                offset += batch_size

            # Update sync details once all data has been inserted
                current_timestamp = pendulum.now("Asia/Kuala_Lumpur")
                dest_cursor.execute("""
                UPDATE data_sync_details
                SET last_sync_date = %s,
                is_load = 'd',initial_sync_date = %s
                WHERE table_name = 'TransactionService_TransactionStatusHistory'
                """, (current_timestamp,formatted_timestamp))
                dest_conn.commit()
                logging.info("Data transfer completed successfully and sync details updated.")

    except Exception as e:
        logging.error("Error in data transfer:", exc_info=True)
        raise
    finally:
        source_cursor.close()
        source_conn.close()
        dest_cursor.close()
        dest_conn.close()

transfer_data_task = PythonOperator(
    task_id='transfer_data_task',
    python_callable=transfer_data,
    dag=dag
)