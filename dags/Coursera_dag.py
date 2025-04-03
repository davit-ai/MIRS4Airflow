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

##Defining the dags

dag = DAG(
    'ETL_toll_data',
    schedule_interval = '@daily',
    default_args = default_args,
    description='Dag Created for assignment provided by Coursera',
    tags=['Coursera','ETL'],
)




def unzip_data():
## File Path
    tar_file_path = r'/opt/airflow/dags/tolldata.tgz'
    extract_path = r'/opt/airflow/dags/extractedFiles'

    ## Opening file and extracting
    with tarfile.open(tar_file_path,'r:gz') as tar:
        tar.extractall(path=extract_path)
        print(f"Files extracted to {extract_path}")

def extract_data_from_csv():
    ## File Path
    file_path = r'/opt/airflow/dags/extractedFiles/vehicle-data.csv'

    ## Reading the csv file and extracting data
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:

                ## Writing the extracted data to a new CSV file
                output_file_path = r'/opt/airflow/dags/extractedFiles/csv_data.csv'
                with open(output_file_path, 'w', newline='') as output_file:
                    fieldnames = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']
                    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in reader:
                        writer.writerow({'Rowid': row[0], 'Timestamp': row[1], 'Anonymized Vehicle number': row[2], 'Vehicle type': row[3]})
                    print(f"Extracted data saved to {output_file_path}")

def extract_data_from_tsv():
        file_path = r'/opt/airflow/dags/extractedFiles/tollplaza-data.tsv'
        output_file_path = r'/opt/airflow/dags/extractedFiles/tsv_data.csv'

        with open(file_path, "r") as readfile, open(output_file_path, "w") as writefile:
            fieldnames = ['Number of axles', 'Tollplaza id', 'Tollplaza code']
            writer = csv.writer(writefile)
            writer.writerow(fieldnames)
            for line in readfile:
                selected_columns = ",".join(line.strip().split("\t")[4:7])
                writefile.write(selected_columns + "\n")
                print(f"Extracted data saved to {output_file_path}")

def extract_data_from_fixed_width():
        file_path = r'/opt/airflow/dags/extractedFiles/payment-data.txt'
        output_file_path = r'/opt/airflow/dags/extractedFiles/fixed_width_data.csv'
        with open(file_path, "r") as readfile, open(output_file_path, "w") as writefile:
            fieldnames = ['Type of Payment code', 'Vehicle Code']
            writer = csv.writer(writefile)
            writer.writerow(fieldnames)
            for line in readfile:
                cleaned_line = " ".join(line.split())
                selected_columns = cleaned_line.split(" ")[9:11]
                writefile.write(",".join(selected_columns) + "\n")


def consolidate_data_extracted():
    file_path = [r'/opt/airflow/dags/extractedFiles/csv_data.csv',
                  r'/opt/airflow/dags/extractedFiles/tsv_data.csv',
                  r'/opt/airflow/dags/extractedFiles/fixed_width_data.csv']
    output_file_path = r'/opt/airflow/dags/extractedFiles/extracted_data.csv'

    combined_csv = pd.concat([pd.read_csv(f) for f in file_path], axis=1)
    combined_csv.to_csv(output_file_path, index=False)
    print(f"Extracted data saved to {output_file_path} after coming all CSV")



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

unzip_data_task >> extract_data_task >> extract_data_task2 >> extract_data_task3 >> extract_data_task4