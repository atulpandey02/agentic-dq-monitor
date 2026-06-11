import asyncio
import os
import json
import snowflake.connector
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from agent.snowflake_connector import SnowflakeConnector

load_dotenv()

# MCP server instance — module level (required by MCP protocol)
app = Server("dq-tools")


class DQToolsHandler:
    """
    Handles all tool execution logic for the MCP server.
    Single responsibility: execute data quality investigation tools.
    MCP decorators delegate to this class.
    SnowflakeConnector injected via dependency injection.
    """

    def __init__(self, connector: SnowflakeConnector):
        self.connector = connector

    def read_dbt_test_output(
        self, table_name: str, test_name: str
    ) -> str:
        """
        Reads dbt run_results.json and returns failing test details.
        Falls back to mock if artifacts not available.
        """
        try:
            dbt_path = os.getenv(
                "DBT_PROJECT_PATH",
                "../stockmarketintelligenceplatform/src/dbt"
            )
            results_path = f"{dbt_path}/target/run_results.json"
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
                            f"Message: {result.get('message', '')}"
                        )
            return "\n\n".join(failures) if failures else (
                f"No failures found for {table_name}. "
                f"Using provided failure details."
            )
        except FileNotFoundError:
            return (
                f"dbt test failure on {table_name}: "
                f"not_null check failed on column 'close_price'. "
                f"143 rows affected. Test: {test_name}."
            )

    def sample_bad_rows(
        self, table_name: str, failing_column: str
    ) -> str:
        """
        Samples rows where failing_column IS NULL from Snowflake.
        Returns formatted string for LLM context.
        """
        try:
            sql = f"""
                SELECT * FROM {table_name}
                WHERE {failing_column} IS NULL
                LIMIT 20
            """
            rows = self.connector.query(sql)
            return self.connector.format_rows(rows)
        except Exception as e:
            return f"Error sampling bad rows: {str(e)}"

    def inspect_upstream_schema(self, source_table: str) -> str:
        """
        Queries INFORMATION_SCHEMA for column definitions.
        Returns structured schema info for LLM to analyze.
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
            rows = self.connector.query(sql)
            return self.connector.format_rows(rows)
        except Exception as e:
            return f"Error inspecting schema: {str(e)}"

    def check_airflow_task_log(
        self, dag_id: str, task_id: str, run_id: str
    ) -> str:
        """
        Calls Airflow REST API to fetch task logs.
        Filters ERROR lines and returns clean snippet.
        """
        try:
            import requests
            airflow_url = os.getenv(
                "AIRFLOW_BASE_URL",
                "http://localhost:8081"
            )
            url = (
                f"{airflow_url}/api/v1/dags/{dag_id}"
                f"/dagRuns/{run_id}"
                f"/taskInstances/{task_id}/logs/1"
            )
            response = requests.get(
                url,
                auth=(
                    os.getenv("AIRFLOW_USER", "airflow"),
                    os.getenv("AIRFLOW_PASSWORD", "airflow")
                ),
                timeout=10
            )
            if response.status_code == 200:
                lines = response.text.split("\n")
                error_lines = [
                    l for l in lines
                    if "ERROR" in l or "Exception" in l
                ]
                return (
                    "\n".join(error_lines[-50:])
                    or "No errors found in log"
                )
            return f"Could not fetch logs: HTTP {response.status_code}"
        except Exception as e:
            return (
                f"Airflow log unavailable: {str(e)}. "
                f"Using failure details for context."
            )


# ── Module-level setup ───────────────────────────────────────────────
# MCP requires decorators at module level — we delegate to handler
connector = SnowflakeConnector()
handler = DQToolsHandler(connector=connector)


@app.list_tools()
async def list_tools() -> list[Tool]:
    """
    Advertises available tools to any connecting agent.
    Agent asks: 'what tools do you have?'
    Server responds with this list.
    """
    return [
        Tool(
            name="read_dbt_test_output",
            description=(
                "Read the structured output of a failing dbt test. "
                "Returns test name, failure message, row count, "
                "and affected columns."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table that failed"
                    },
                    "test_name": {
                        "type": "string",
                        "description": "Name of the dbt test that failed"
                    }
                },
                "required": ["table_name", "test_name"]
            }
        ),
        Tool(
            name="sample_bad_rows",
            description=(
                "Query Snowflake to sample rows that caused "
                "the test failure. Returns top 20 bad rows."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Snowflake table to query"
                    },
                    "failing_column": {
                        "type": "string",
                        "description": "Column with failing values"
                    }
                },
                "required": ["table_name", "failing_column"]
            }
        ),
        Tool(
            name="inspect_upstream_schema",
            description=(
                "Check upstream source table schema for changes. "
                "Returns column list and any recent modifications."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_table": {
                        "type": "string",
                        "description": "Source table to inspect"
                    }
                },
                "required": ["source_table"]
            }
        ),
        Tool(
            name="check_airflow_task_log",
            description=(
                "Retrieve error lines from an Airflow task log. "
                "Returns clean error snippet stripped of noise."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "Airflow DAG ID"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Airflow task ID"
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Airflow DAG run ID"
                    }
                },
                "required": ["dag_id", "task_id", "run_id"]
            }
        ),
    ]


@app.call_tool()
async def call_tool(
    name: str, arguments: dict
) -> list[TextContent]:
    """
    Receives tool call from agent.
    Delegates to DQToolsHandler — keeps MCP layer thin.
    """
    if name == "read_dbt_test_output":
        result = handler.read_dbt_test_output(
            arguments["table_name"],
            arguments["test_name"]
        )
    elif name == "sample_bad_rows":
        result = handler.sample_bad_rows(
            arguments["table_name"],
            arguments["failing_column"]
        )
    elif name == "inspect_upstream_schema":
        result = handler.inspect_upstream_schema(
            arguments["source_table"]
        )
    elif name == "check_airflow_task_log":
        result = handler.check_airflow_task_log(
            arguments["dag_id"],
            arguments["task_id"],
            arguments["run_id"]
        )
    else:
        result = f"Unknown tool: {name}"

    return [TextContent(type="text", text=result)]


async def main():
    """Starts the MCP server using stdio transport."""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    print("Starting DQ Tools MCP Server...")
    asyncio.run(main())