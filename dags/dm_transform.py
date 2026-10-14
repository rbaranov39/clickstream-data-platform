"""Трансформации слоя DM: сборка стендовых витрин.

Даты для расчета выручки берутся из последнего слепка заказов и списка
пропущенных дней. Сверка покупок с заказами учитывает также прожитые дни
событий и дни, где заказ еще не найден. Запросы записывают результаты
в промежуточные таблицы, затем заменяют дневные разделы основных таблиц.

Трафик пересчитывается за всю историю: новая связь посетителя с пользователем
магазина меняет число известных пользователей и за прошлые дни.
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Variable, dag, task, task_group

START_DATE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)

SQL_ROOT = Path("/opt/airflow/sql")
CLICKHOUSE_CONNECTION = "clickhouse_default"
# Та же ось модельного времени, что у DDS. Общего модуля у дагов нет
# намеренно: связь держит переменная Airflow, а не импорт одного дага другим.
WORLD_POSITION = "world_position"


def _world_position() -> int:
    """Позиция мира для границы прожитых событий сверки.

    Причина отдельного чтения переменной разобрана у одноименной функции в
    ``dds_transform.py``.
    """
    raw_position = Variable.get(WORLD_POSITION, default=None)
    if raw_position is None:
        raise AirflowException(
            "мир еще не создан: выполните make up и дождитесь world_initialize"
        )
    try:
        position = int(raw_position)
    except (TypeError, ValueError) as error:
        raise AirflowException(
            f"world_position={raw_position!r} не является номером дня; "
            "выполните make rebuild-storage"
        ) from error
    if position < 1:
        raise AirflowException(
            f"мир не готов: world_position={position}; "
            "выполните make up или make rebuild-storage"
        )
    return position


def _days(first_day: datetime.date, last_day: datetime.date) -> list[str]:
    """Дни отрезка строками - в таком виде их ждет замена партиции."""
    return [
        (first_day + datetime.timedelta(days=shift)).isoformat()
        for shift in range((last_day - first_day).days + 1)
    ]


@task
def replacements(scope: dict[str, object]) -> list[dict[str, object]]:
    """Подготовить параметры отдельной задачи replace для каждого дня.

    expand_kwargs получает список словарей и создает по одному экземпляру
    задачи на каждый словарь. Дата подставляется в SQL шаблоном Jinja:
    параметр ClickHouse в ALTER ... PARTITION здесь не поддерживается.
    """
    return [{"params": {"day": day}} for day in scope["days"]]


@dag(
    dag_id="dm_transform",
    schedule=None,
    start_date=START_DATE,
    is_paused_upon_creation=False,
    max_active_runs=1,
    template_searchpath=str(SQL_ROOT),
    tags=["dm"],
)
def dm_transform():
    """Пересобрать стендовые витрины по текущему состоянию DDS."""

    @task_group(group_id="revenue_daily")
    def revenue_daily():
        """Выручка: изменяемое окно заказов и пропущенная история."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у хранилища, какие дни пересобирает запуск."""
            query = (SQL_ROOT / "dm" / "revenue_daily_scope.sql").read_text(
                encoding="utf-8"
            )
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query)
            if row is None:
                raise AirflowException(
                    "нет слепка заказов в ODS или готовых заказов в DDS; "
                    "дождитесь загрузки слепка и сначала выполните dds_transform"
                )
            first_day, last_day = row
            days = _days(first_day, last_day)
            logging.info(
                "объем пересборки: %s … %s, дней %d",
                first_day,
                last_day,
                len(days),
            )
            return {
                "first_day": first_day.isoformat(),
                "last_day": last_day.isoformat(),
                "days": days,
            }

        days = scope()

        rebuild = SQLExecuteQueryOperator(
            task_id="rebuild",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/revenue_daily_rebuild.sql",
            parameters={
                "load_id": "{{ run_id }}",
                "first_day": days["first_day"],
                "last_day": days["last_day"],
            },
            split_statements=True,
            do_xcom_push=False,
        )

        replace = SQLExecuteQueryOperator.partial(
            task_id="replace",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/revenue_daily_replace.sql",
            do_xcom_push=False,
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    revenue_daily()

    @task_group(group_id="purchase_vs_orders")
    def purchase_vs_orders():
        """Сверка: изменяемые заказы, прожитые покупки и дни ожидания заказа."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у обоих источников, какие дни пересобрать."""
            query = (SQL_ROOT / "dm" / "purchase_vs_orders_scope.sql").read_text(
                encoding="utf-8"
            )
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query, parameters={"position": _world_position()})
            if row is None:
                raise AirflowException(
                    "нет принятого слепка заказов или прожитых покупок; "
                    "дождитесь источников и сначала выполните dds_transform"
                )
            first_day, last_day = row
            days = _days(first_day, last_day)
            logging.info(
                "объем пересборки: %s … %s, дней %d",
                first_day,
                last_day,
                len(days),
            )
            return {
                "first_day": first_day.isoformat(),
                "last_day": last_day.isoformat(),
                "days": days,
            }

        days = scope()

        rebuild = SQLExecuteQueryOperator(
            task_id="rebuild",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/purchase_vs_orders_rebuild.sql",
            parameters={
                "load_id": "{{ run_id }}",
                "first_day": days["first_day"],
                "last_day": days["last_day"],
            },
            split_statements=True,
            do_xcom_push=False,
        )

        replace = SQLExecuteQueryOperator.partial(
            task_id="replace",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/purchase_vs_orders_replace.sql",
            do_xcom_push=False,
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    purchase_vs_orders()

    @task_group(group_id="top_pages_daily")
    def top_pages_daily():
        """Топ страниц: пересборка событий с учетом пропущенных дней."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у хранилища, какие дни пересобирает запуск."""
            query = (SQL_ROOT / "dm" / "top_pages_daily_scope.sql").read_text(
                encoding="utf-8"
            )
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query)

            first_day, last_day = row
            if last_day is None:
                raise AirflowException(
                    "DDS пуст; "
                    "дождитесь загрузки слепка и сначала выполните dds_transform"
                )
            if first_day is None:
                raise AirflowSkipException("все прожитые дни уже собраны")

            days = _days(first_day, last_day)
            logging.info(
                "объем пересборки: %s … %s, дней %d",
                first_day,
                last_day,
                len(days),
            )
            return {
                "first_day": first_day.isoformat(),
                "last_day": last_day.isoformat(),
                "days": days,
            }

        days = scope()

        rebuild = SQLExecuteQueryOperator(
            task_id="rebuild",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/top_pages_daily_rebuild.sql",
            parameters={
                "load_id": "{{ run_id }}",
                "first_day": days["first_day"],
                "last_day": days["last_day"],
            },
            split_statements=True,
            do_xcom_push=False,
        )

        replace = SQLExecuteQueryOperator.partial(
            task_id="replace",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/top_pages_daily_replace.sql",
            do_xcom_push=False,
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    top_pages_daily()

    @task_group(group_id="session_overview")
    def session_overview():
        """Обзор сессий: пересборка сессий с учетом пропущенных дней."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у хранилища, какие дни пересобирает запуск."""
            query = (SQL_ROOT / "dm" / "session_overview_scope.sql").read_text(
                encoding="utf-8"
            )
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            row = hook.get_first(query)

            first_day, last_day = row
            if last_day is None:
                raise AirflowException(
                    "DDS пуст; "
                    "дождитесь загрузки слепка и сначала выполните dds_transform"
                )
            if first_day is None:
                raise AirflowSkipException("все прожитые дни уже собраны")

            days = _days(first_day, last_day)
            logging.info(
                "объем пересборки: %s … %s, дней %d",
                first_day,
                last_day,
                len(days),
            )
            return {
                "first_day": first_day.isoformat(),
                "last_day": last_day.isoformat(),
                "days": days,
            }

        days = scope()

        rebuild = SQLExecuteQueryOperator(
            task_id="rebuild",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/session_overview_rebuild.sql",
            parameters={
                "load_id": "{{ run_id }}",
                "first_day": days["first_day"],
                "last_day": days["last_day"],
            },
            split_statements=True,
            do_xcom_push=False,
        )

        replace = SQLExecuteQueryOperator.partial(
            task_id="replace",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/session_overview_replace.sql",
            do_xcom_push=False,
        ).expand_kwargs(replacements(days))

        rebuild >> replace

    session_overview()

    @task_group(group_id="dq_errors_daily")
    def dq_errors_daily():
        """Ошибки качества данных: пересборка только существующих дней."""

        @task
        def scope() -> dict[str, object]:
            """Спросить у хранилища, какие дни пересобирает запуск."""
            query = (SQL_ROOT / "dm" / "dq_errors_daily_scope.sql").read_text(
                encoding="utf-8"
            )
            hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONNECTION)
            rows = hook.get_records(query)

            if not rows:
                raise AirflowSkipException("нет ошибок для пересборки")

            days = [row[0].isoformat() for row in rows]
            logging.info("объем пересборки: %d дней", len(days))
            return {"days": days}

        days = scope()

        rebuild = SQLExecuteQueryOperator(
            task_id="rebuild",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/dq_errors_daily_rebuild.sql",
            parameters={"load_id": "{{ run_id }}"},
            split_statements=True,
            do_xcom_push=False,
        )

        replace = SQLExecuteQueryOperator.partial(
            task_id="replace",
            conn_id=CLICKHOUSE_CONNECTION,
            sql="dm/dq_errors_daily_replace.sql",
            do_xcom_push=False,
        ).expand_kwargs(replacements(days))

        days >> rebuild >> replace

    dq_errors_daily()

    SQLExecuteQueryOperator(
        task_id="daily_traffic",
        conn_id=CLICKHOUSE_CONNECTION,
        sql="dm/daily_traffic_rebuild.sql",
        parameters={"load_id": "{{ run_id }}"},
        do_xcom_push=False,
    )


dm_transform()
