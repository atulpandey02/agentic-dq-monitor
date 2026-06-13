import requests
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

AGENT_API_URL = "http://host.docker.internal:8090"



def run_dbt_tests(**context):
    """
    Triggers DQ agent when called.
    dbt runs on host machine — this task calls the agent API
    which reads the real run_results.json from the host.
    """
    print(f"Triggering DQ agent at {datetime.utcnow().isoformat()}")

    dag_run_conf = context.get('dag_run').conf or {}
    failure_details = dag_run_conf.get(
        'failure_details',
        'source_not_null_batch_HISTORICAL_STOCK_CLOSE_PRICE failed. Got 1 result.'
    )
    table_name = dag_run_conf.get('table_name', 'HISTORICAL_STOCK')

    try:
        response = requests.post(
            f"{AGENT_API_URL}/trigger",
            json={
                "failure_type": "dbt_test",
                "table_name": table_name,
                "failure_details": failure_details,
                "pipeline_name": "stock_market_batch_pipeline"
            },
            timeout=300
        )
        result_data = response.json()
        run_id = result_data.get("run_id")

        print(f"Agent triggered successfully.")
        print(f"Run ID: {run_id}")
        print(f"Status: {result_data.get('status')}")

        context['task_instance'].xcom_push(
            key='agent_run_id',
            value=run_id
        )

    except Exception as e:
        print(f"Failed to trigger agent: {e}")
        raise

    else:
        print("All dbt tests passed. No agent trigger needed.")
        print("dbt tests passed. Pipeline healthy.")


# ── DAG Definition ────────────────────────────────────────────────────
with DAG(
    dag_id="dbt_test_with_agent_trigger",
    description="Runs dbt tests and auto-triggers DQ agent on failure",
    schedule_interval="0 * * * *",  # every hour
    start_date=days_ago(1),
    catchup=False,
    tags=["data-quality", "dbt", "agentic"],
    default_args={
        "owner": "atul",
        "retries": 0,
    }
) as dag:

    dbt_test_task = PythonOperator(
        task_id="run_dbt_tests_and_trigger_agent",
        python_callable=run_dbt_tests,
        provide_context=True,
    )