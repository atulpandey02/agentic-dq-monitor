import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Agent API running on your Mac
AGENT_API_URL = "http://host.docker.internal:8090"


def trigger_dq_agent(**context):
    """
    Calls the agent API to start investigation.
    Agent runs on Mac — Airflow just triggers it via HTTP.
    Pushes run_id to XCom for resume task.
    """
    dag_run_conf = context.get('dag_run').conf or {}

    failure_type = dag_run_conf.get('failure_type', 'dbt_test')
    table_name = dag_run_conf.get('table_name', 'HISTORICAL_STOCK')
    failure_details = dag_run_conf.get(
        'failure_details',
        'not_null check failed on close_price. 143 rows affected.'
    )
    pipeline_name = dag_run_conf.get(
        'pipeline_name',
        'stock_market_batch_pipeline'
    )

    print(f"Triggering DQ agent for: {table_name}")
    print(f"Failure type: {failure_type}")

    response = requests.post(
        f"{AGENT_API_URL}/trigger",
        json={
            "failure_type": failure_type,
            "table_name": table_name,
            "failure_details": failure_details,
            "pipeline_name": pipeline_name
        },
        timeout=300    # agent investigation can take up to 5 mins
    )

    response.raise_for_status()
    result = response.json()
    run_id = result["run_id"]

    print(f"Agent paused at HITL. Run ID: {run_id}")

    # Push run_id to XCom so resume task can use it
    context['task_instance'].xcom_push(
        key='agent_run_id',
        value=run_id
    )


def resume_dq_agent(**context):
    """
    Calls the agent API to resume after human decision.
    Engineer triggers this task manually via Airflow UI.
    """
    # Pull run_id from trigger task via XCom
    run_id = context['task_instance'].xcom_pull(
        task_ids='trigger_dq_agent',
        key='agent_run_id'
    )

    dag_run_conf = context.get('dag_run').conf or {}
    decision = dag_run_conf.get('human_decision', 'approved')
    feedback = dag_run_conf.get('feedback', '')

    print(f"Resuming agent run: {run_id}")
    print(f"Human decision: {decision}")

    response = requests.post(
        f"{AGENT_API_URL}/resume",
        json={
            "run_id": run_id,
            "decision": decision,
            "feedback": feedback
        },
        timeout=120
    )

    response.raise_for_status()
    result = response.json()

    print(f"Agent run complete: {result['message']}")


# ── DAG Definition ────────────────────────────────────────────────────
with DAG(
    dag_id="dq_monitor",
    description="Agentic Data Quality Monitor — triggers on pipeline failures",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["data-quality", "agentic", "llm"],
    default_args={
        "owner": "atul",
        "retries": 0,
    }
) as dag:

    trigger_task = PythonOperator(
        task_id="trigger_dq_agent",
        python_callable=trigger_dq_agent,
        provide_context=True,
    )

    resume_task = PythonOperator(
        task_id="resume_dq_agent",
        python_callable=resume_dq_agent,
        provide_context=True,
    )

    trigger_task >> resume_task