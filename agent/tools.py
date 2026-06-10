from langchain_core.tools import tool


class DataQualityTools:
    """
    All 4 investigation tools the agent can call.
    Each tool has a single responsibility.
    Week 1: All tools return mocked data.
    Week 2: Replace mock returns with real Snowflake/Airflow calls.
    """

    @staticmethod
    @tool
    def read_dbt_test_output(table_name: str, test_name: str) -> str:
        """
        Read the structured output of a failing dbt test.
        Returns test name, failure message, row count, affected columns.
        """
        # MOCK — Week 2 will read from real dbt run_results.json
        return (
            f"dbt test failure on {table_name}: "
            f"not_null check failed on column 'close_price'. "
            f"143 rows affected. Test: {test_name}."
        )

    @staticmethod
    @tool
    def sample_bad_rows(table_name: str, failing_column: str) -> str:
        """
        Query Snowflake to sample rows that caused the test failure.
        Returns top 20 bad rows as formatted string.
        """
        # MOCK — Week 2 will run real Snowflake query
        return (
            f"Sample bad rows from {table_name} where {failing_column} is NULL:\n"
            f"date=2024-01-15, symbol=AAPL, close_price=NULL, volume=1500000\n"
            f"date=2024-01-15, symbol=MSFT, close_price=NULL, volume=2300000\n"
            f"date=2024-01-15, symbol=GOOGL, close_price=NULL, volume=980000\n"
            f"Total NULL rows: 143"
        )

    @staticmethod
    @tool
    def inspect_upstream_schema(source_table: str) -> str:
        """
        Check if the upstream source table schema changed recently.
        Returns any column additions, removals, or type changes.
        """
        # MOCK — Week 2 will query Snowflake INFORMATION_SCHEMA
        return (
            f"Schema inspection for {source_table}: "
            f"No schema changes detected. "
            f"All expected columns present. "
            f"Last schema change: 30 days ago."
        )

    @staticmethod
    @tool
    def check_airflow_task_log(dag_id: str, task_id: str, run_id: str) -> str:
        """
        Retrieve relevant error lines from an Airflow task log.
        Returns clean error snippet stripped of noise.
        """
        # MOCK — Week 2 will call Airflow REST API
        return (
            f"Airflow log for {dag_id}/{task_id} run {run_id}:\n"
            f"[ERROR] Task failed with exception: NullPointerException\n"
            f"[ERROR] Source data missing for date partition 2024-01-15\n"
            f"[INFO] Retried 3 times, all failed."
        )


# Export tools as a list for LangGraph to consume
def get_tools() -> list:
    """Returns all tools as a flat list for binding to the LLM."""
    return [
        DataQualityTools.read_dbt_test_output,
        DataQualityTools.sample_bad_rows,
        DataQualityTools.inspect_upstream_schema,
        DataQualityTools.check_airflow_task_log,
    ]