from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from airflow.hooks.base_hook import BaseHook
from io import StringIO
import os
import tarfile
import csv
import pandas as pd

## Defining the default arguments
default_args = {
    'owner': 'Davit Khanal',
    'start_date':datetime.now(),
    'email':'khanaldavit@gmail.com',
    'email_on_failure': False, ## Set true in production
    'email_on_retry': False, ## Set true in production
    'retries':3,
    'retry_delay' : timedelta(minutes=5)
}


## DESTINATION = "/home/project/airflow/dags/finalassignment"
DESTINATION = "/opt/airflow/dags"
source = os.path.join(DESTINATION, "tolldata.tgz")
vehicle_data = os.path.join(DESTINATION, "vehicle-data.csv")
tollplaza_data = os.path.join(DESTINATION, "tollplaza-data.tsv")
payment_data = os.path.join(DESTINATION, "payment-data.txt")
csv_data = os.path.join(DESTINATION, "csv_data.csv")
tsv_data = os.path.join(DESTINATION, "tsv_data.csv")
fixed_width_data = os.path.join(DESTINATION, "fixed_width_data.csv")
extracted_data = os.path.join(DESTINATION, "extracted_data.csv")
transformed_data = os.path.join(DESTINATION, "staging/transformed_data.csv")
##Defining the dags

dag = DAG(
    'ETL_toll_data',
    schedule_interval = '@daily',
    default_args = default_args,
    description='Dag Created for assignment provided by Coursera',
    tags=['Coursera','ETL'],
)

def unzip_data():
    ## Opening file and extracting
    with tarfile.open(source,'r:gz') as tar:
        tar.extractall(path=DESTINATION)
        print(f"Files extracted to {DESTINATION}")

def extract_data_from_csv():
    ## Reading the csv file and extracting data
    with open(vehicle_data, 'r') as file:
        reader = csv.reader(file)
        for row in reader:

                ## Writing the extracted data to a new CSV file
                with open(csv_data, 'w', newline='') as output_file:
                    fieldnames = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']
                    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in reader:
                        writer.writerow({'Rowid': row[0], 'Timestamp': row[1], 'Anonymized Vehicle number': row[2], 'Vehicle type': row[3]})


def extract_data_from_tsv():
        with open(tollplaza_data, "r") as readfile, open(tsv_data, "w") as writefile:
            fieldnames = ['Number of axles', 'Tollplaza id', 'Tollplaza code']
            writer = csv.writer(writefile)
            writer.writerow(fieldnames)
            for line in readfile:
                selected_columns = ",".join(line.strip().split("\t")[4:7])
                writefile.write(selected_columns + "\n")


def extract_data_from_fixed_width():
        with open(payment_data, "r") as readfile, open(fixed_width_data, "w") as writefile:
            fieldnames = ['Type of Payment code', 'Vehicle Code']
            writer = csv.writer(writefile)
            writer.writerow(fieldnames)
            for line in readfile:
                cleaned_line = " ".join(line.split())
                selected_columns = cleaned_line.split(" ")[9:11]
                writefile.write(",".join(selected_columns) + "\n")


def consolidate_data_extracted():
    file_path = [csv_data,
                  tsv_data,
                  fixed_width_data]
    combined_csv = pd.concat([pd.read_csv(f) for f in file_path], axis=1)
    combined_csv.to_csv(extracted_data, index=False)



def transform_data():
    with open(extracted_data, 'r') as file, open(transformed_data, 'w', newline='') as output_file:
        reader = csv.reader(file)
        writer = csv.writer(output_file)

        for row in reader:
            row[3] = row[3].upper()
            writer.writerow(row)


## Defining the function to be used in the dag
unzip_data_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag,
)

extract_data_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_data_task2 = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_data_task3 = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

extract_data_task4 = PythonOperator(
    task_id='consolidate_data_extracted',
    python_callable=consolidate_data_extracted,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

unzip_data_task >> extract_data_task >> extract_data_task2 >> extract_data_task3 >> extract_data_task4 >> transform_data