
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'vamsha',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="EDP_sales_customers_to_Sandpit_Sales_customers",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    catchup=False,
    tags=['auto-generated'],
) as dag:

    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extracting data from EDP.sales.customers"'
    )

    load = BashOperator(
        task_id='load_data',
        bash_command='echo "Loading data into Sandpit.Sales.customers"'
    )

    extract >> load