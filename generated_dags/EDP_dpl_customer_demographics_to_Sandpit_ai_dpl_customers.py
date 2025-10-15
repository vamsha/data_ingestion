
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
    dag_id="EDP_dpl_customer_demographics_to_Sandpit_ai_dpl_customers",
    default_args=default_args,
    schedule_interval="everyday at 7pm",
    catchup=False,
    is_paused_upon_creation=False,  # ✅ Auto-enable when added
    tags=['auto-generated'],
) as dag:

    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extracting data from EDP.dpl.customer_demographics"'
    )

    load = BashOperator(
        task_id='load_data',
        bash_command='echo "Loading data into Sandpit.ai_dpl.customers"'
    )

    extract >> load