import json
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from evaluation.judge import DiagnosisJudge

load_dotenv()


class EvaluationRunner:
    """
    Runs all evaluation scenarios through the diagnosis pipeline
    and scores results using LLM-as-judge.
    Single responsibility: orchestrate evaluation runs.
    """

    def __init__(self):
        self.judge = DiagnosisJudge()
        self.llm = ChatGroq(
            model="llama-3.3-70b-versatile",
            temperature=0
        )
        self.results = []

    def _run_diagnosis(self, scenario: dict) -> tuple[str, str]:
        """
        Runs the diagnosis prompt against a scenario.
        Returns (predicted_category, predicted_explanation)
        """
        prompt_template = open(
            "agent/prompts/v1_diagnose.txt"
        ).read()

        prompt = prompt_template.format(
            table_name=scenario["table_name"],
            failure_type=scenario["failure_type"],
            failure_details=scenario["failure_details"],
            bad_row_sample=scenario["bad_row_sample"],
            upstream_schema=scenario["upstream_schema"],
            airflow_log_snippet="Not available"
        )

        response = self.llm.invoke(prompt)
        raw = response.content

        # Parse CATEGORY and EXPLANATION
        category = "UNKNOWN"
        explanation = raw

        for line in raw.split("\n"):
            if line.startswith("CATEGORY:"):
                category = line.replace("CATEGORY:", "").strip()
            elif line.startswith("EXPLANATION:"):
                explanation = line.replace("EXPLANATION:", "").strip()

        return category, explanation

    def _run_propose_fix(self, scenario: dict, root_cause: str) -> str:
        """
        Runs the propose fix prompt against a scenario.
        Returns proposed_fix string.
        """
        prompt_template = open(
            "agent/prompts/v1_propose.txt"
        ).read()

        prompt = prompt_template.format(
            root_cause=root_cause,
            table_name=scenario["table_name"],
            failure_type=scenario["failure_type"],
            bad_row_sample=scenario["bad_row_sample"]
        )

        response = self.llm.invoke(prompt)
        raw = response.content

        # Strip markdown if present
        import re
        match = re.search(
            r"```(?:sql)?\s*(.*?)```",
            raw,
            re.DOTALL
        )
        if match:
            return match.group(1).strip()
        return raw.strip()

    def run_all(self) -> list:
        """
        Runs all scenarios and returns scored results.
        """
        scenarios_path = "evaluation/scenarios.json"
        with open(scenarios_path) as f:
            scenarios = json.load(f)

        print(f"\n{'='*60}")
        print(f"EVALUATION RUN — {datetime.utcnow().isoformat()}")
        print(f"Scenarios: {len(scenarios)}")
        print(f"Agent model: llama-3.3-70b-versatile")
        print(f"Judge model: llama-3.1-8b-instant")
        print(f"{'='*60}\n")

        for scenario in scenarios:
            print(f"Running scenario: {scenario['id']}...")

            # Step 1: Run diagnosis
            category, explanation = self._run_diagnosis(scenario)
            print(f"  Predicted: {category}")
            print(f"  Expected:  {scenario['expected_category']}")

            # Step 2: Run fix proposal
            proposed_fix = self._run_propose_fix(
                scenario,
                f"CATEGORY: {category}\nEXPLANATION: {explanation}"
            )

            # Step 3: Judge scores the result
            scores = self.judge.evaluate(
                scenario=scenario,
                predicted_category=category,
                predicted_explanation=explanation,
                proposed_fix=proposed_fix
            )

            scores["proposed_fix"] = proposed_fix[:200]
            self.results.append(scores)

            # Print result
            status = "✅" if scores["category_correct"] else "❌"
            print(f"  {status} Category correct: {scores['category_correct']}")
            print(f"  📊 Explanation quality: {scores['explanation_quality']}/5")
            print(f"  🔧 Fix correct: {scores['fix_correct']}")
            print(f"  💬 Feedback: {scores['feedback']}")
            print()

        self._print_summary()
        self._save_to_snowflake()
        return self.results

    def _print_summary(self):
        """Prints evaluation summary."""
        total = len(self.results)
        correct = sum(1 for r in self.results if r["category_correct"])
        avg_quality = sum(
            r["explanation_quality"] for r in self.results
        ) / total
        fix_correct = sum(1 for r in self.results if r["fix_correct"])

        print(f"\n{'='*60}")
        print(f"EVALUATION SUMMARY")
        print(f"{'='*60}")
        print(f"Category accuracy:    {correct}/{total} ({correct/total*100:.0f}%)")
        print(f"Avg explanation quality: {avg_quality:.1f}/5")
        print(f"Fix accuracy:         {fix_correct}/{total} ({fix_correct/total*100:.0f}%)")
        print(f"{'='*60}\n")

    def _save_to_snowflake(self):
        """Saves eval results to DQ_MONITOR.AUDIT.EVAL_RESULTS."""
        try:
            from agent.snowflake_connector import SnowflakeConnector
            connector = SnowflakeConnector()
            eval_run_id = str(uuid.uuid4())

            for result in self.results:
                sql = f"""
                    INSERT INTO DQ_MONITOR.AUDIT.EVAL_RESULTS (
                        eval_id, scenario_id, run_id,
                        category_correct, explanation_quality,
                        judge_feedback, evaluated_at
                    ) VALUES (
                        '{str(uuid.uuid4())}',
                        '{result["scenario_id"]}',
                        '{eval_run_id}',
                        {result["category_correct"]},
                        {result["explanation_quality"]},
                        $${result.get("feedback", "")}$$,
                        '{datetime.utcnow().isoformat()}'
                    )
                """
                connector.execute(sql, database="DQ_MONITOR")

            print(f"✅ Eval results saved to Snowflake.")
            connector.close()

        except Exception as e:
            print(f"⚠️  Could not save to Snowflake: {e}")
            print("Results printed above are still valid.")


if __name__ == "__main__":
    runner = EvaluationRunner()
    runner.run_all()