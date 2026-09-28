"""Custom Airflow operator that wraps the data quality check logic."""
from typing import List

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityGateOperator(BaseOperator):
    """
    Runs data quality checks against a PostgreSQL table.
    Fails the task (and optionally the DAG) if checks don't pass.

    :param conn_id: Airflow Postgres connection ID
    :param table: Fully qualified table name (schema.table)
    :param checks: List of check dicts: {"sql": "...", "expected_result": value}
    :param fail_on_error: If True, raise exception on failure (default True)
    """

    template_fields = ("table",)
    ui_color = "#f0ede4"

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        table: str,
        checks: List[dict],
        fail_on_error: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table = table
        self.checks = checks
        self.fail_on_error = fail_on_error

    def execute(self, context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        failed_checks = []

        for check in self.checks:
            sql = check["sql"].format(table=self.table)
            expected = check["expected_result"]
            result = hook.get_first(sql)[0]

            if result != expected:
                failed_checks.append({
                    "check": check.get("name", sql[:50]),
                    "expected": expected,
                    "actual": result,
                })
                self.log.error(
                    f"Quality check FAILED on {self.table}: "
                    f"expected={expected}, actual={result} | SQL: {sql}"
                )
            else:
                self.log.info(f"Quality check PASSED: {check.get('name', 'check')} = {result}")

        if failed_checks and self.fail_on_error:
            raise ValueError(
                f"Data quality gate failed on {self.table}: {len(failed_checks)} check(s) failed. "
                f"Details: {failed_checks}"
            )

        return {"table": self.table, "checks_run": len(self.checks), "failed": len(failed_checks)}
