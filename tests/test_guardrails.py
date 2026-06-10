import pytest
from agent.guardrails import GuardrailChecker


class TestGuardrailChecker:
    """
    Unit tests for all 4 guardrail rules.
    Each test has one responsibility — tests one specific rule.
    """

    def setup_method(self):
        """Runs before every test. Creates a fresh GuardrailChecker."""
        self.checker = GuardrailChecker()
        self.table = "HISTORICAL_STOCK"

    # ─── RULE 1: Destructive Keywords ───────────────────────────────

    def test_blocks_drop_table(self):
        fix = "DROP TABLE HISTORICAL_STOCK"
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False
        assert "DROP TABLE" in reason

    def test_blocks_truncate(self):
        fix = "TRUNCATE TABLE HISTORICAL_STOCK"
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False
        assert "TRUNCATE" in reason

    def test_blocks_delete(self):
        fix = "DELETE FROM HISTORICAL_STOCK WHERE close_price IS NULL"
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False
        assert "DELETE FROM" in reason

    def test_blocks_alter_table(self):
        fix = "ALTER TABLE HISTORICAL_STOCK ADD COLUMN new_col VARCHAR"
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False
        assert "ALTER TABLE" in reason

    def test_blocks_drop_database(self):
        fix = "DROP DATABASE STOCKMARKETBATCH"
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False
        assert "DROP DATABASE" in reason

    # ─── RULE 2: Table Reference ─────────────────────────────────────

    def test_blocks_wrong_table_reference(self):
        fix = "UPDATE SOME_OTHER_TABLE SET close_price = 0.0 WHERE close_price IS NULL"
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False
        assert "HISTORICAL_STOCK" in reason

    def test_passes_correct_table_reference(self):
        """This test only checks Rule 2 directly."""
        result = self.checker._check_table_reference(
            f"UPDATE {self.table} SET close_price = 0.0",
            self.table
        )
        assert result[0] is True

    # ─── RULE 3: Single Statement ─────────────────────────────────────

    def test_blocks_multiple_statements(self):
        fix = (
            "UPDATE HISTORICAL_STOCK SET close_price = 0.0 "
            "WHERE close_price IS NULL; "
            "UPDATE HISTORICAL_STOCK SET volume = 0 WHERE volume IS NULL"
        )
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False
        assert "multiple" in reason.lower()

    def test_passes_single_statement(self):
        """This test only checks Rule 3 directly."""
        result = self.checker._check_single_statement(
            "UPDATE HISTORICAL_STOCK SET close_price = 0.0 WHERE close_price IS NULL"
        )
        assert result[0] is True

    # ─── RULE 1: Edge Cases ───────────────────────────────────────────

    def test_blocks_drop_in_lowercase(self):
        """Guardrail must catch lowercase destructive keywords too."""
        fix = "drop table HISTORICAL_STOCK"
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False

    def test_blocks_create_or_replace(self):
        fix = "CREATE OR REPLACE TABLE HISTORICAL_STOCK AS SELECT * FROM backup"
        passed, reason = self.checker.check(fix, self.table)
        assert passed is False

    # ─── SAFE FIX: Should Pass All Rules ─────────────────────────────

    def test_safe_update_passes_rules_1_2_3(self):
        """
        A clean UPDATE statement should pass Rules 1, 2, and 3.
        We test rules individually to avoid Rule 4 LLM call in unit tests.
        """
        fix = "UPDATE HISTORICAL_STOCK SET close_price = 0.0 WHERE close_price IS NULL"

        rule1 = self.checker._check_destructive_keywords(fix)
        rule2 = self.checker._check_table_reference(fix, self.table)
        rule3 = self.checker._check_single_statement(fix)

        assert rule1[0] is True, f"Rule 1 failed: {rule1[1]}"
        assert rule2[0] is True, f"Rule 2 failed: {rule2[1]}"
        assert rule3[0] is True, f"Rule 3 failed: {rule3[1]}"