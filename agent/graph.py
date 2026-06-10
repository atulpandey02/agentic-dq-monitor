import uuid
from datetime import datetime
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import ToolNode

from agent.state import AgentState
from agent.tools import DataQualityTools, get_tools
from agent.guardrails import GuardrailChecker

load_dotenv()


class DataQualityAgent:
    """
    LangGraph stateful agent that investigates and remediates
    data quality failures in the stock market pipeline.
    Single responsibility: orchestrate the full investigation flow.
    """

    def __init__(self):
        self.llm = ChatGroq(
            model="llama-3.3-70b-versatile",
            temperature=0
        )
        self.tools = get_tools()
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        self.guardrail_checker = GuardrailChecker()
        self.graph = self._build_graph()

    # ----------------------------------------------------------------
    # NODE DEFINITIONS
    # Each node reads from state, does one job, returns state updates
    # ----------------------------------------------------------------

    def investigate_node(self, state: AgentState) -> dict:
        """
        Node 1: ReAct loop — LLM decides which tools to call
        based on the failure context.
        """
        print(">> investigate_node running")
        response = self.llm_with_tools.invoke(state["messages"])
        return {"messages": state["messages"] + [response]}

    def diagnose_node(self, state: AgentState) -> dict:
        """
        Node 2: Synthesize tool results into a root cause diagnosis.
        Reads prompt from versioned file.
        """
        print(">> diagnose_node running")
        prompt_template = open("agent/prompts/v1_diagnose.txt").read()
        prompt = prompt_template.format(
            table_name=state["table_name"],
            failure_type=state["failure_type"],
            failure_details=state["failure_details"],
            bad_row_sample=state.get("bad_row_sample", "Not available"),
            upstream_schema=state.get("upstream_schema", "Not available"),
            airflow_log_snippet=state.get("airflow_log_snippet", "Not available"),
        )
        response = self.llm.invoke(prompt)
        return {"root_cause": response.content}

    def propose_fix_node(self, state: AgentState) -> dict:
        """
        Node 3: Propose a specific SQL or config remediation.
        Reads prompt from versioned file.
        """
        print(">> propose_fix_node running")
        prompt_template = open("agent/prompts/v1_propose.txt").read()
        prompt = prompt_template.format(
            root_cause=state.get("root_cause", "Unknown"),
            table_name=state["table_name"],
            failure_type=state["failure_type"],
            bad_row_sample=state.get("bad_row_sample", "Not available"),
        )
        response = self.llm.invoke(prompt)
        raw = response.content

        # Strip markdown code blocks if LLM wrapped the SQL
        import re
        match = re.search(r"```(?:sql)?\s*(.*?)```", raw, re.DOTALL)
        if match:
            cleaned = match.group(1).strip()
        else:
            cleaned = raw.strip()

        print(f"   Cleaned fix: {cleaned}")
        return {"proposed_fix": cleaned}

    def guardrail_node(self, state: AgentState) -> dict:
        """
        Node 4: Run all 4 safety checks on the proposed fix.
        Blocks destructive operations before human sees them.
        """
        print(">> guardrail_node running")
        passed, reason = self.guardrail_checker.check(
            state["proposed_fix"],
            state["table_name"]
        )
        return {
            "guardrail_passed": passed,
            "guardrail_reason": reason
        }

    def human_approval_node(self, state: AgentState) -> dict:
        """
        Node 5: HITL checkpoint.
        Graph pauses BEFORE this node via interrupt_before.
        When resumed, this node runs and prints the summary.
        """
        print(">> human_approval_node running")
        print(f"   Root cause: {state.get('root_cause')}")
        print(f"   Proposed fix: {state.get('proposed_fix')}")
        print(f"   Guardrail: {state.get('guardrail_reason')}")
        print(f"   Human decision: {state.get('human_decision')}")
        return {
            "human_decision": state.get("human_decision"),
            "human_feedback": state.get("human_feedback")
    }

    def execute_fix_node(self, state: AgentState) -> dict:
        """
        Node 6: Execute the fix ONLY if human approved.
        Week 2: replace print with real Snowflake execution.
        """
        print(">> execute_fix_node running")
        if state.get("human_decision") == "approved":
            print(f"   Executing fix: {state.get('proposed_fix')}")
            # Week 2: real Snowflake execution goes here
        else:
            print(f"   Fix rejected by engineer.")
        return state

    # ----------------------------------------------------------------
    # ROUTING FUNCTIONS
    # Conditional edges — decide which node to go to next
    # ----------------------------------------------------------------

    def route_after_guardrail(self, state: AgentState) -> str:
        """After guardrail: go to human approval or block."""
        print(f"   DEBUG guardrail_passed: {state.get('guardrail_passed')}")
        print(f"   DEBUG proposed_fix: {state.get('proposed_fix')}")
        if state.get("guardrail_passed"):
            return "human_approval"
        return END

    def route_after_human(self, state: AgentState) -> str:
        """After human decision: execute or end."""
        if state.get("human_decision") == "approved":
            return "execute_fix"
        return END

    # ----------------------------------------------------------------
    # GRAPH BUILDER
    # Wires all nodes and edges together
    # ----------------------------------------------------------------

    def _build_graph(self):
        """Assembles the full LangGraph StateGraph."""
        builder = StateGraph(AgentState)

        # Register all nodes
        builder.add_node("investigate", self.investigate_node)
        builder.add_node("tools", ToolNode(self.tools))
        builder.add_node("diagnose", self.diagnose_node)
        builder.add_node("propose_fix", self.propose_fix_node)
        builder.add_node("guardrail", self.guardrail_node)
        builder.add_node("human_approval", self.human_approval_node)
        builder.add_node("execute_fix", self.execute_fix_node)

        # Wire the edges
        builder.set_entry_point("investigate")
        builder.add_edge("investigate", "tools")
        builder.add_edge("tools", "diagnose")
        builder.add_edge("diagnose", "propose_fix")
        builder.add_edge("propose_fix", "guardrail")
        builder.add_edge("execute_fix", END)

        # Conditional edges
        builder.add_conditional_edges(
            "guardrail",
            self.route_after_guardrail
        )
        builder.add_conditional_edges(
            "human_approval",
            self.route_after_human
        )

        # Compile with HITL interrupt and memory checkpointer
        checkpointer = MemorySaver()
        return builder.compile(
            checkpointer=checkpointer,
            interrupt_before=["human_approval"]
        )

    # ----------------------------------------------------------------
    # PUBLIC INTERFACE
    # How you trigger and resume the agent
    # ----------------------------------------------------------------

    def run(self, failure_type: str, table_name: str,
            failure_details: str, pipeline_name: str) -> str:
        """
        Trigger a new agent run.
        Returns the run_id — use it to resume after human approval.
        """
        run_id = str(uuid.uuid4())
        config = {"configurable": {"thread_id": run_id}}

        initial_state = {
            "run_id": run_id,
            "failure_type": failure_type,
            "failure_details": failure_details,
            "table_name": table_name,
            "pipeline_name": pipeline_name,
            "started_at": datetime.utcnow().isoformat(),
            "messages": [
                HumanMessage(content=(
                    f"Pipeline failure detected. "
                    f"Type: {failure_type}. "
                    f"Table: {table_name}. "
                    f"Details: {failure_details}"
                ))
            ]
        }

        print(f"\n{'='*50}")
        print(f"Agent run started. ID: {run_id}")
        print(f"{'='*50}\n")

        for event in self.graph.stream(initial_state, config=config):
            print(f"Event: {list(event.keys())}")

        print(f"\nAgent paused at HITL checkpoint.")
        print(f"Run ID: {run_id}")
        print(f"Call resume(run_id, 'approved') to continue.\n")

        return run_id

    def resume(self, run_id: str, decision: str,
               feedback: str = "") -> None:
        """
        Resume a paused agent run after human decision.
        decision: 'approved' or 'rejected'
        """
        config = {"configurable": {"thread_id": run_id}}

        print(f"\nResuming run {run_id} with decision: {decision}\n")

        # Update the state with human decision
        self.graph.update_state(
            config,
            {"human_decision": decision, "human_feedback": feedback}
        )

        # Resume from where it paused — pass None to continue
        for event in self.graph.stream(None, config=config):
            print(f"Event: {list(event.keys())}")

        print("\nAgent run complete.")