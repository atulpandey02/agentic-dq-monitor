from langchain_groq import ChatGroq
from dotenv import load_dotenv

load_dotenv()


class GuardrailChecker:
    """
    Safety layer that validates any proposed fix before it
    reaches the human or gets executed.
    Single responsibility: block unsafe operations.
    """

    BLOCKED_KEYWORDS = [
        "DROP TABLE",
        "DROP DATABASE",
        "TRUNCATE",
        "DELETE FROM",
        "ALTER TABLE",
        "GRANT",
        "REVOKE",
        "CREATE OR REPLACE",
    ]

    def __init__(self):
        self.llm = ChatGroq(
            model="llama-3.3-70b-versatile",
            temperature=0
        )

    def check(self, proposed_fix: str, table_name: str) -> tuple[bool, str]:
        """
        Runs all 4 safety checks on the proposed fix.
        Returns (passed: bool, reason: str)
        """
        # Rule 1: Block destructive keywords
        result = self._check_destructive_keywords(proposed_fix)
        if not result[0]:
            return result

        # Rule 2: Fix must reference the correct table
        result = self._check_table_reference(proposed_fix, table_name)
        if not result[0]:
            return result

        # Rule 3: Only single statements allowed
        result = self._check_single_statement(proposed_fix)
        if not result[0]:
            return result

        # Rule 4: LLM safety check for edge cases
        result = self._check_llm_safety(proposed_fix, table_name)
        if not result[0]:
            return result

        return True, "All guardrail checks passed"

    def _check_destructive_keywords(
        self, proposed_fix: str
    ) -> tuple[bool, str]:
        """Rule 1: Block any destructive SQL operations."""
        fix_upper = proposed_fix.upper()
        for keyword in self.BLOCKED_KEYWORDS:
            if keyword in fix_upper:
                return (
                    False,
                    f"Blocked: fix contains destructive operation '{keyword}'"
                )
        return True, "No destructive keywords found"

    def _check_table_reference(
        self, proposed_fix: str, table_name: str
    ) -> tuple[bool, str]:
        """Rule 2: Fix must only reference the failing table."""
        if table_name.upper() not in proposed_fix.upper():
            return (
                False,
                f"Blocked: fix does not reference the failing table '{table_name}'"
            )
        return True, "Table reference valid"

    def _check_single_statement(
        self, proposed_fix: str
    ) -> tuple[bool, str]:
        """Rule 3: Only one SQL statement allowed — no batched operations."""
        statements = [
            s.strip()
            for s in proposed_fix.split(";")
            if s.strip()
        ]
        if len(statements) > 1:
            return (
                False,
                "Blocked: fix contains multiple SQL statements"
            )
        return True, "Single statement confirmed"

    def _check_llm_safety(
        self, proposed_fix: str, table_name: str
    ) -> tuple[bool, str]:
        """Rule 4: LLM double-checks for edge cases the rules might miss."""
        prompt = f"""
        Is this SQL fix safe to run on a production Snowflake table?
        Fix: {proposed_fix}
        Table: {table_name}
        Answer only YES or NO followed by a one-line reason.
        """
        response = self.llm.invoke(prompt)
        if response.content.strip().upper().startswith("NO"):
            return (
                False,
                f"LLM safety check failed: {response.content}"
            )
        return True, "LLM safety check passed"