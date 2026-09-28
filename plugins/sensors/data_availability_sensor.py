"""Sensor that waits until source data is available before triggering ETL."""

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class DataAvailabilitySensor(BaseSensorOperator):
    """
    Polls a PostgreSQL table until a minimum row count condition is met.
    Use to gate ETL start until upstream data has landed.

    :param conn_id: Airflow Postgres connection ID
    :param table: Fully qualified table name to check
    :param min_rows: Minimum rows required to proceed
    :param date_column: Optional date column to filter to logical_date
    """

    template_fields = ("table", "date_column")
    ui_color = "#c8f0c8"

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        table: str,
        min_rows: int = 1,
        date_column: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table = table
        self.min_rows = min_rows
        self.date_column = date_column

    def poke(self, context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        logical_date = context["logical_date"].date()

        if self.date_column:
            sql = f"SELECT COUNT(*) FROM {self.table} WHERE {self.date_column}::DATE = '{logical_date}'"
        else:
            sql = f"SELECT COUNT(*) FROM {self.table}"

        try:
            row_count = hook.get_first(sql)[0]
            self.log.info(
                f"[{self.table}] row count = {row_count} (need >= {self.min_rows})"
            )
            return row_count >= self.min_rows
        except Exception as e:
            self.log.warning(f"Sensor poke failed: {e}")
            return False
