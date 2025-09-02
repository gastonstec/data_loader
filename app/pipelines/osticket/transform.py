# app/pipelines/pbxadaptix/transform.py
from loguru import logger
from datetime import date, time, datetime
from .work_time_calc import WorkingTimeCalculator
import duckdb

PIPENAME = "osticket"
TIMEOFFSET = "6 hours"

SLA_THRESHOLDS_MIN = {
    "Emergency": 240,
    "Low": 1620,
    "Medium": 1080,
    "High": 540
}


# Function to adjust timestamps to Mexico City time
def set_fechas_mx(table_name: str):
    try:
        # Set Mexico City time for creation and closure dates
        duckdb.execute(
            (
                f"UPDATE {table_name} "
                f"SET fecha_creacion_mx = "
                f"fecha_creacion - INTERVAL '{TIMEOFFSET}', "
                f"fecha_cierre_mx = fecha_cierre - INTERVAL '{TIMEOFFSET}';"
            )
        )
    except Exception as e:
        logger.error(f"Error setting fechas for {PIPENAME} data: {e}")


def calculate_elapsed_time(table_name: str):
    # Create calculator with schedule
    calculator = WorkingTimeCalculator(
        working_shifts=[
            # Morning Shift
            (time(8, 0), time(13, 0)),
            # Afternoon Shift
            (time(14, 0), time(18, 0))
        ],
        # Saturday and Sunday are weekends
        weekends=[6, 7],
        # Specify holidays
        holidays=[
            date(2025, 1, 1), date(2025, 2, 5),
            date(2025, 3, 21), date(2025, 5, 1), date(2025, 9, 16),
            date(2025, 11, 17), date(2025, 12, 25)]
    )

    try:
        results = duckdb.sql(
            "SELECT numero_ticket, fecha_creacion_mx, fecha_cierre_mx "
            f"FROM {table_name}"
        ).fetchall()
        for r in results:
            start_datetime = datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S")
            end_datetime = datetime.strptime(r[2], "%Y-%m-%d %H:%M:%S")
            minutes = calculator.calculate_working_time(
                start_dt=start_datetime,
                end_dt=end_datetime,
                unit="minutes"
            )
            duckdb.sql(
                f"UPDATE {table_name} SET elapsed_time_minutes = {minutes} "
                f"WHERE numero_ticket = '{r[0]}';"
            )
    except Exception as e:
        print(f"Error calculating elapsed time: {e}")
        return


def calculate_sla_met(table_name: str):
    try:
        results = duckdb.sql(
            "SELECT numero_ticket, prioridad, sla_elapsed_time_minutes "
            f"FROM {table_name}"
        ).fetchall()


        for r in results:
            if r[1]
            if r[1] in SLA_THRESHOLDS_MIN:
                SLA_THRESHOLD = SLA_THRESHOLDS_MIN[r[1]]
                if r[2] <= SLA_THRESHOLD:
                    duckdb.sql(
                        f"UPDATE {table_name} SET sla_met = TRUE "
                        f"WHERE numero_ticket = '{r[0]}';"
                    )
                else:
                    duckdb.sql(
                        f"UPDATE {table_name} SET sla_met = FALSE "
                        f"WHERE numero_ticket = '{r[0]}';"
                    )
    except Exception as e:
        print(f"Error calculating elapsed time: {e}")
        return


def transform(table_name: str):
    try:
        # Set Mexico City time for creation and closure dates
        set_fechas_mx(table_name)
        # Calculate elapsed time in working minutes
        calculate_elapsed_time(table_name)
        # Fetch results
        results = duckdb.sql(
            (
                f"SELECT fecha_creacion, fecha_cierre, fecha_creacion_mx, "
                f"fecha_cierre_mx, sla_elapsed_time_minutes "
                f"FROM {table_name};"
            )
        )
        print(results)
    except Exception as e:
        logger.error(f"Error transforming {PIPENAME} data: {e}")


def start(db_pool, table_list: list[str]):
    for table in table_list:
        transform(table)
