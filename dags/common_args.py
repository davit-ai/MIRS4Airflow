from datetime import datetime

# Define the default_args that are common for all DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 27),  # You can change this to a dynamic date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1
}