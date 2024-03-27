import importlib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'aiq_etl_scheduler',
    default_args=default_args,
    description='A DAG to schedule Python ETL scripts',
    schedule_interval='@daily',  # Adjust the schedule interval as needed
)


def run_sales_order(script_name, **kwargs):
    print(script_name)
    module = importlib.import_module(script_name)
    module.main()


def run_customer_and_weather(script_name, **kwargs):
    module = importlib.import_module(script_name)
    module.main()

run_sales_order_task = PythonOperator(
    task_id='run_sales_order',
    python_callable=run_sales_order,
    op_kwargs={'script_name': 'sales_order'},  # Pass the script name as a keyword argument
    provide_context=True,
    dag=dag,
)


run_customer_and_weather_task = PythonOperator(
    task_id='run_customer_and_weather',
    python_callable=run_customer_and_weather,
    op_kwargs={'script_name': 'customer_and_weather'},
    dag=dag,
)

# Set task dependencies
run_sales_order_task >> run_customer_and_weather_task
