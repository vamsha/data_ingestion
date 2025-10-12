
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
    dag_id="hana_v1_tran_ftr_to_hive_tran_ftr",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    catchup=False,
    tags=['auto-generated'],
) as dag:

    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extracting data from hana_v1.tran.ftr"'
    )

    load = BashOperator(
        task_id='load_data',
        bash_command='echo "Loading data into hive.tran.ftr"'
    )

    extract >> load