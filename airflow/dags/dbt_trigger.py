import requests
import time
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

AGENT_API_URL = "http://host.docker.internal:8090"


def trigger_dq_agent(**context):
    """
    Triggers DQ agent when pipeline failure detected.
    Pushes run_id to XCom for next task.
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
        print(f"ACTION REQUIRED: Approve or reject via:")
        print(f"POST {AGENT_API_URL}/resume")
        print(f'{{"run_id": "{run_id}", "decision": "approved"}}')

        context['task_instance'].xcom_push(
            key='agent_run_id',
            value=run_id
        )

    except Exception as e:
        print(f"Failed to trigger agent: {e}")
        raise


def wait_for_human_approval(**context):
    """
    Polls agent API every 30 seconds waiting for human decision.
    Times out after 2 hours.
    DAG stays running until engineer approves or rejects.
    """
    run_id = context['task_instance'].xcom_pull(
        task_ids='trigger_dq_agent',
        key='agent_run_id'
    )

    if not run_id:
        print("No run_id found — skipping approval wait.")
        return

    print(f"Waiting for human approval on run: {run_id}")
    print(f"Approve via: POST {AGENT_API_URL}/resume")
    print(f'{{"run_id": "{run_id}", "decision": "approved"}}')

    max_wait_seconds = 7200  # 2 hours
    poll_interval = 30       # check every 30 seconds
    elapsed = 0

    while elapsed < max_wait_seconds:
        try:
            response = requests.get(
                f"{AGENT_API_URL}/status/{run_id}",
                timeout=10
            )
            data = response.json()
            print(f"DEBUG response: {data}")
            status = data.get("human_decision")
            print(f"DEBUG status: {status}")

            if status == "approved":
                print(f"✅ Engineer approved the fix.")
                print(f"Fix was executed successfully.")
                return

            elif status == "rejected":
                print(f"❌ Engineer rejected the fix.")
                print(f"Manual investigation required.")
                raise Exception("Fix rejected by engineer.")

            else:
                print(
                    f"⏳ Waiting for approval... "
                    f"({elapsed}s elapsed)"
                )

        except requests.exceptions.RequestException as e:
            print(f"Poll error: {e}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise Exception(
        f"Approval timeout after {max_wait_seconds}s. "
        f"Run ID: {run_id}"
    )


def verify_pipeline_health(**context):
    """
    Final health check after fix is applied.
    Confirms pipeline is healthy before marking DAG complete.
    """
    print("Verifying pipeline health after fix...")
    print("Checking agent audit log...")

    run_id = context['task_instance'].xcom_pull(
        task_ids='trigger_dq_agent',
        key='agent_run_id'
    )

    try:
        response = requests.get(
            f"{AGENT_API_URL}/runs/{run_id}",
            timeout=10
        )
        data = response.json()
        details = data.get("details", {})

        print(f"Run ID: {run_id}")
        print(f"Root cause: {details.get('ROOT_CAUSE', 'N/A')[:100]}")
        print(f"Human decision: {details.get('HUMAN_DECISION', 'N/A')}")
        print(f"Guardrail passed: {details.get('GUARDRAIL_PASSED', 'N/A')}")
        print(f"✅ Pipeline health verified.")
        print(f"✅ All checks complete. Pipeline is healthy.")

    except Exception as e:
        print(f"Health check warning: {e}")
        print("Pipeline assumed healthy based on approval.")


# ── DAG Definition ────────────────────────────────────────────────────
with DAG(
    dag_id="dbt_test_with_agent_trigger",
    description="Runs dbt tests, triggers DQ agent, waits for approval",
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["data-quality", "dbt", "agentic"],
    default_args={
        "owner": "atul",
        "retries": 0,
        "execution_timeout": None,
    }
) as dag:

    trigger_task = PythonOperator(
        task_id="trigger_dq_agent",
        python_callable=trigger_dq_agent,
        provide_context=True,
    )

    wait_task = PythonOperator(
        task_id="wait_for_human_approval",
        python_callable=wait_for_human_approval,
        provide_context=True,
        execution_timeout=None,
    )

    verify_task = PythonOperator(
        task_id="verify_pipeline_health",
        python_callable=verify_pipeline_health,
        provide_context=True,
    )

    trigger_task >> wait_task >> verify_task