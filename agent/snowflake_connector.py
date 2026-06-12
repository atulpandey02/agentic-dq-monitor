import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


class SnowflakeConnector:
    """
    Manages Snowflake connection lifecycle.
    Single responsibility: connect, query, close.
    Injected into DataQualityTools via dependency injection.
    """

    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.role = os.getenv("SNOWFLAKE_ROLE")
        self._conn = None

    def connect(self) -> None:
        """Opens a Snowflake connection."""
        self._conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            role=self.role
        )
        print(">> SnowflakeConnector: connected")

    def query(self, sql: str, database: str = "STOCKMARKETBATCH") -> list:
        """
        Executes a SELECT query.
        Always creates a fresh connection to avoid token expiry.
        """
        fresh_conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            role=self.role
        )
        cursor = fresh_conn.cursor(snowflake.connector.DictCursor)
        try:
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()
            fresh_conn.close()

    def execute(self, sql: str, database: str = "STOCKMARKETBATCH") -> None:
        """
        Executes a non-SELECT statement.
        Always creates a fresh connection to avoid token expiry.
        """
        # Always create fresh connection for execution
        # Prevents authentication token expiry on long-running sessions
        fresh_conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            role=self.role
        )
        cursor = fresh_conn.cursor()
        try:
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(sql)
            fresh_conn.commit()
            print(f">> SnowflakeConnector: executed successfully")
        finally:
            cursor.close()
            fresh_conn.close()

    def close(self) -> None:
        """Closes the connection cleanly."""
        if self._conn:
            self._conn.close()
            self._conn = None
            print(">> SnowflakeConnector: connection closed")

    def format_rows(self, rows: list) -> str:
        """
        Formats query results as a readable string for agent context.
        Converts list of dicts to readable text the LLM can reason about.
        """
        if not rows:
            return "No rows returned"

        lines = []
        for row in rows[:20]:  # cap at 20 rows
            line = ", ".join(f"{k}={v}" for k, v in row.items())
            lines.append(line)

        return f"{len(rows)} rows found:\n" + "\n".join(lines)