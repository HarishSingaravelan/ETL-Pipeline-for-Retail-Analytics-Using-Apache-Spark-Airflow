from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",  # Who owns/maintains this DAG
    "depends_on_past": False,  # Tasks don't depend on past runs
    "start_date": datetime(2024, 9, 23),  # When the DAG should start running
    "email_on_failure": False,  # Don't send emails on task failure
    "email_on_retry": False,  # Don't send emails on task retries
    "retries": 1,  # Number of times to retry a failed task
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Create the DAG object
dag = DAG(
    "example_dag1",  # Unique identifier for the DAG
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),  # Run the DAG once per day
    catchup=False,  # Don't run for historical dates when DAG is first enabled
    tags=["example"],  # Tags for organizing DAGs in the Airflow UI
)


# Define a simple Python function that prints "Hello World"
def print_hello():
    print("Hello World")


# Create a Python task that will run the print_hello function
hello_operator = PythonOperator(
    task_id="hello_task",  # Unique identifier for this task
    python_callable=print_hello,  # Function to execute
    dag=dag,
)

# Create a Bash task that will run the 'date' command
date_operator = BashOperator(
    task_id="date_task",  # Unique identifier for this task
    bash_command="date",  # Shell command to execute
    dag=dag,
)

# Define task dependencies: hello_task runs before date_task
hello_operator >> date_operator

# This enables running the DAG file directly for testing
if __name__ == "__main__":
    dag.cli()
