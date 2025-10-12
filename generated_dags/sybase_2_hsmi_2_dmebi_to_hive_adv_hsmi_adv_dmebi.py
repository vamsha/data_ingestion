
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
    dag_id="sybase_2_hsmi_2_dmebi_to_hive_adv_hsmi_adv_dmebi",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['auto-generated'],
) as dag:

    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extracting data from sybase_2.hsmi_2.dmebi"'
    )

    load = BashOperator(
        task_id='load_data',
        bash_command='echo "Loading data into hive.adv_hsmi.adv_dmebi"'
    )

    extract >> load