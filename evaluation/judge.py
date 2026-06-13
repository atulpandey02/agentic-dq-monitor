import json
import os
from dotenv import load_dotenv
from langchain_groq import ChatGroq

load_dotenv()


class DiagnosisJudge:
    """
    LLM-as-judge evaluator for agent diagnosis quality.
    Single responsibility: score agent diagnoses against
    expected outcomes using a separate LLM as the judge.
    """

    def __init__(self):
        # Different model family than agent (Gemini vs Llama)
        # Prevents self-grading bias — industry best practice
        # Free tier: 15 requests/min, 1500 requests/day
        self.llm = ChatGroq(
            model="llama-3.1-8b-instant",
            temperature=0
        )

    def evaluate(
        self,
        scenario: dict,
        predicted_category: str,
        predicted_explanation: str,
        proposed_fix: str
    ) -> dict:
        """
        Scores the agent's diagnosis on three dimensions:
        1. Category accuracy (correct/incorrect)
        2. Explanation quality (1-5)
        3. Fix appropriateness (correct/incorrect)
        """
        prompt = f"""
You are evaluating an AI data engineering agent's diagnosis.

SCENARIO:
- Table: {scenario['table_name']}
- Failure type: {scenario['failure_type']}
- Failure details: {scenario['failure_details']}
- Bad row sample: {scenario['bad_row_sample']}
- Upstream schema: {scenario['upstream_schema']}

EXPECTED:
- Category: {scenario['expected_category']}
- Fix should contain: {scenario['expected_fix_contains']}

AGENT PREDICTED:
- Category: {predicted_category}
- Explanation: {predicted_explanation}
- Proposed fix: {proposed_fix}

Score the agent on THREE dimensions:

1. CATEGORY_CORRECT: Did the agent classify correctly?
   true if predicted matches expected, false otherwise.

2. EXPLANATION_QUALITY: How good is the explanation? (1-5)
   5 = perfect, identifies exact root cause with evidence
   4 = good, mostly correct with minor gaps
   3 = acceptable, correct direction but vague
   2 = poor, partially correct but missing key insight
   1 = wrong, explanation does not match the evidence

3. FIX_CORRECT: Is the proposed fix appropriate?
   true if fix contains '{scenario['expected_fix_contains']}',
   false otherwise.

Respond ONLY with valid JSON, no markdown, no explanation:
{{"category_correct": true/false, "explanation_quality": 1-5, "fix_correct": true/false, "feedback": "one sentence"}}
"""
        response = self.llm.invoke(prompt)

        # Parse JSON response safely
        try:
            # Strip any markdown if LLM adds it
            content = response.content.strip()
            if "```" in content:
                import re
                match = re.search(
                    r"```(?:json)?\s*(.*?)```",
                    content,
                    re.DOTALL
                )
                if match:
                    content = match.group(1).strip()

            scores = json.loads(content)
            scores["scenario_id"] = scenario["id"]
            scores["predicted_category"] = predicted_category
            scores["expected_category"] = scenario["expected_category"]
            return scores

        except json.JSONDecodeError:
            return {
                "scenario_id": scenario["id"],
                "category_correct": False,
                "explanation_quality": 0,
                "fix_correct": False,
                "feedback": f"Judge failed to parse: {response.content[:100]}",
                "predicted_category": predicted_category,
                "expected_category": scenario["expected_category"]
            }