from typing import TypedDict, Optional, List


class AgentState(TypedDict):
    # --- Trigger context ---
    # What woke the agent up
    failure_type: str        # "dbt_test" | "airflow_dag" | "schema_change"
    failure_details: str     # Raw failure output from dbt or Airflow
    table_name: str          # Which table failed
    pipeline_name: str       # Which DAG or pipeline

    # --- Investigation results ---
    # What the agent found using its 4 tools
    bad_row_sample: Optional[str]       # Sample of bad rows from Snowflake
    upstream_schema: Optional[str]      # Schema of upstream source table
    airflow_log_snippet: Optional[str]  # Relevant Airflow log lines
    root_cause: Optional[str]           # Agent's diagnosis

    # --- Remediation ---
    # What the agent proposes and what happens next
    proposed_fix: Optional[str]         # SQL or config change proposed
    guardrail_passed: Optional[bool]    # Did the fix pass safety checks
    guardrail_reason: Optional[str]     # Why it failed if it did
    human_decision: Optional[str]       # "approved" | "rejected"
    human_feedback: Optional[str]       # Engineer's comment

    # --- LLMOps ---
    # Observability and audit fields
    run_id: str                         # Unique ID for this agent run
    started_at: str                     # ISO timestamp
    token_count: Optional[int]          # Total tokens used
    messages: List[dict]                # Full message history