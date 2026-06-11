import uvicorn
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import Optional
from agent.graph import DataQualityAgent
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Agentic DQ Monitor API",
    description="REST API for triggering and resuming the DQ agent",
    version="1.0.0"
)

# Single agent instance — shared across requests
agent = DataQualityAgent()


class TriggerRequest(BaseModel):
    """Request model for triggering a new agent run."""
    failure_type: str
    table_name: str
    failure_details: str
    pipeline_name: str = "stock_market_batch_pipeline"


class ResumeRequest(BaseModel):
    """Request model for resuming a paused agent run."""
    run_id: str
    decision: str        # "approved" | "rejected"
    feedback: Optional[str] = ""


class TriggerResponse(BaseModel):
    """Response after triggering agent."""
    run_id: str
    status: str
    message: str


class ResumeResponse(BaseModel):
    """Response after resuming agent."""
    run_id: str
    status: str
    message: str


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "healthy", "service": "agentic-dq-monitor"}


@app.post("/trigger", response_model=TriggerResponse)
def trigger_agent(request: TriggerRequest):
    """
    Triggers a new agent investigation run.
    Called by Airflow when dbt tests fail.
    Returns run_id for resuming after HITL pause.
    """
    print(f"\nTrigger received: {request.failure_type} on {request.table_name}")

    run_id = agent.run(
        failure_type=request.failure_type,
        table_name=request.table_name,
        failure_details=request.failure_details,
        pipeline_name=request.pipeline_name
    )

    return TriggerResponse(
        run_id=run_id,
        status="paused",
        message=f"Agent paused at HITL checkpoint. Call /resume with run_id to continue."
    )


@app.post("/resume", response_model=ResumeResponse)
def resume_agent(request: ResumeRequest):
    """
    Resumes a paused agent run after human decision.
    Engineer calls this with approved/rejected decision.
    """
    print(f"\nResume received: {request.run_id} — {request.decision}")

    agent.resume(
        run_id=request.run_id,
        decision=request.decision,
        feedback=request.feedback
    )

    return ResumeResponse(
        run_id=request.run_id,
        status="completed",
        message=f"Agent run completed with decision: {request.decision}"
    )


@app.get("/runs/{run_id}")
def get_run_status(run_id: str):
    """
    Check status of an agent run from audit log.
    """
    from agent.snowflake_connector import SnowflakeConnector
    connector = SnowflakeConnector()
    try:
        rows = connector.query(
            f"SELECT * FROM AUDIT.AGENT_RUNS WHERE run_id = '{run_id}'",
            database="DQ_MONITOR"
        )
        if rows:
            return {"run_id": run_id, "status": "found", "details": rows[0]}
        return {"run_id": run_id, "status": "not_found"}
    except Exception as e:
        return {"run_id": run_id, "status": "error", "message": str(e)}
    finally:
        connector.close()


if __name__ == "__main__":
    uvicorn.run(
        "agent.api:app",
        host="0.0.0.0",
        port=8090,
        reload=True
    )