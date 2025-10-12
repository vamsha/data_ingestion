import pandas as pd
from jinja2 import Template
from datetime import datetime, timedelta
import os

def read_config(config_csv_path: str) -> pd.DataFrame:
    """Read CSV config into a DataFrame."""
    return pd.read_csv(config_csv_path)

def get_latest_config_row(df: pd.DataFrame) -> pd.Series:
    """Get the latest row from the config DataFrame."""
    return df.iloc[-1]

def create_dag_id(row: pd.Series) -> str:
    """Create DAG ID based on the config row."""
    return f"{row['source_system']}_{row['source_schema']}_{row['source_data']}_to_{row['target_system']}_{row['target_schema']}_{row['target_data']}"

def render_dag_template(dag_id: str, row: pd.Series) -> str:
    """Render the DAG template with the provided config."""
    dag_template = """
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
    dag_id="{{ dag_id }}",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['auto-generated'],
) as dag:

    extract = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extracting data from {{ source_system }}.{{ source_schema }}.{{ source_data }}"'
    )

    load = BashOperator(
        task_id='load_data',
        bash_command='echo "Loading data into {{ target_system }}.{{ target_schema }}.{{ target_data }}"'
    )

    extract >> load
"""
    return Template(dag_template).render(
        dag_id=dag_id,
        source_system=row['source_system'],
        source_schema=row['source_schema'],
        source_data=row['source_data'],
        target_system=row['target_system'],
        target_schema=row['target_schema'],
        target_data=row['target_data'],
    )

def save_dag(rendered_dag: str, output_dir: str, dag_id: str):
    """Save the rendered DAG to the output directory."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{dag_id}.py")
    with open(output_path, "w") as f:
        f.write(rendered_dag)
    print(f"✅ DAG saved to {output_path}")

def generate_dag(config_csv_path: str, output_dir: str):
    df = read_config(config_csv_path)
    latest_row = get_latest_config_row(df)
    dag_id = create_dag_id(latest_row)
    rendered_dag = render_dag_template(dag_id, latest_row)
    save_dag(rendered_dag, output_dir, dag_id)

def main():
    config_csv_path = "config/ingestion.csv"
    output_dir = "generated_dags"

    print(f"Generating DAG from last row of config: {config_csv_path}")
    print(f"Output directory: {output_dir}")

    try:
        generate_dag(config_csv_path, output_dir)
    except Exception as e:
        print(f"❌ Error generating DAG: {e}")

if __name__ == "__main__":
    main()
