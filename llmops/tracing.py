import os
from dotenv import load_dotenv

load_dotenv()

os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = "agentic-dq-monitor"
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY")


def get_project_name() -> str:
    return os.environ["LANGCHAIN_PROJECT"]