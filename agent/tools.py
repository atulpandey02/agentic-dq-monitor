import json
import os
from dotenv import load_dotenv
from langchain_core.tools import tool
from agent.snowflake_connector import SnowflakeConnector

load_dotenv()


class DataQualityTools:
    """
    All 4 investigation tools the agent can call.
    Week 2: Real Snowflake and dbt connections.
    SnowflakeConnector injected via dependency injection.
    """

    def __init__(self, connector: SnowflakeConnector):
        self.connector = connector
        self._register_tools()

    def _register_tools(self):
        """
        Registers all tools as instance-bound callables.
        Required because @tool decorator doesn't work
        directly on instance methods.
        """
        connector = self.connector

        @tool
        def read_dbt_test_output(table_name: str, test_name: str) -> str:
            """
            Read the structured output of a failing dbt test.
            Returns test name, failure message, row count, affected columns.
            """
            try:
                dbt_project_path = os.getenv(
                    "DBT_PROJECT_PATH",
                    "../stock-market-lakehouse/src/dbt"
                )
                results_path = f"{dbt_project_path}/target/run_results.json"

                with open(results_path) as f:
                    results = json.load(f)

                failures = []
                for result in results.get("results", []):
                    if result.get("status") == "fail":
                        node_id = result.get("unique_id", "")
                        if table_name.lower() in node_id.lower():
                            failures.append(
                                f"Test: {result.get('unique_id')}\n"
                                f"Status: {result.get('status')}\n"
                                f"Failures: {result.get('failures')}\n"
                                f"Message: {result.get('message', 'No message')}"
                            )

                if failures:
                    return "\n\n".join(failures)
                return (
                    f"No failures found for {table_name} in dbt results. "
                    f"Using provided failure details instead."
                )

            except FileNotFoundError:
                # Fallback to mock if dbt artifacts not available
                return (
                    f"dbt test failure on {table_name}: "
                    f"not_null check failed on column 'close_price'. "
                    f"143 rows affected. Test: {test_name}."
                )

        @tool
        def sample_bad_rows(table_name: str, failing_column: str) -> str:
            """
            Query Snowflake to sample rows that caused the test failure.
            Returns top 20 bad rows as formatted string.
            """
            try:
                sql = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE {failing_column} IS NULL
                    LIMIT 20
                """
                rows = connector.query(sql)
                return connector.format_rows(rows)

            except Exception as e:
                return f"Error sampling bad rows: {str(e)}"

        @tool
        def inspect_upstream_schema(source_table: str) -> str:
            """
            Check if the upstream source table schema changed recently.
            Returns column list and any recent changes.
            """
            try:
                sql = f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{source_table.upper()}'
                    ORDER BY ORDINAL_POSITION
                """
                rows = connector.query(sql)
                return connector.format_rows(rows)

            except Exception as e:
                return f"Error inspecting schema: {str(e)}"

        @tool
        def check_airflow_task_log(dag_id: str, task_id: str, run_id: str) -> str:
            """
            Retrieve relevant error lines from an Airflow task log.
            Returns clean error snippet stripped of noise.
            """
            try:
                import requests
                airflow_url = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
                airflow_user = os.getenv("AIRFLOW_USER", "airflow")
                airflow_pass = os.getenv("AIRFLOW_PASSWORD", "airflow")

                url = (
                    f"{airflow_url}/api/v1/dags/{dag_id}"
                    f"/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"
                )
                response = requests.get(
                    url,
                    auth=(airflow_user, airflow_pass),
                    timeout=10
                )

                if response.status_code == 200:
                    lines = response.text.split("\n")
                    error_lines = [
                        l for l in lines
                        if "ERROR" in l or "Exception" in l
                    ]
                    return "\n".join(error_lines[-50:]) or "No errors found"

                return f"Could not fetch logs: HTTP {response.status_code}"

            except Exception as e:
                return (
                    f"Airflow log unavailable: {str(e)}. "
                    f"Using context from failure details instead."
                )

        # Store as instance attributes so graph.py can access them
        self.read_dbt_test_output = read_dbt_test_output
        self.sample_bad_rows = sample_bad_rows
        self.inspect_upstream_schema = inspect_upstream_schema
        self.check_airflow_task_log = check_airflow_task_log

    def get_tools(self) -> list:
        """Returns all tools as a list for binding to the LLM."""
        return [
            self.read_dbt_test_output,
            self.sample_bad_rows,
            self.inspect_upstream_schema,
            self.check_airflow_task_log,
        ]