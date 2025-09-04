# app/pipelines/pbxadaptix/transform.py
from loguru import logger
from datetime import date, time, datetime
import decimal
import duckdb
from .work_time_calc import WorkingTimeCalculator
from .pipelineinfo import PIPENAME


TIMEOFFSET = "6 hours"

SLA_THRESHOLDS_MIN = {
    "Emergency": 240.0,
    "Low": 1620.0,
    "Medium": 1080.0,
    "High": 540.0
}


# Function to adjust timestamps to Mexico City time
def set_fechas_mx(table_name: str):
    try:
        # Set Mexico City time for creation date
        duckdb.execute(
            (
                f"UPDATE {table_name} "
                f"SET fecha_creacion_mx = "
                f"fecha_creacion - INTERVAL '{TIMEOFFSET}';"
            )
        )
        # Set Mexico City time for closed tickets
        duckdb.execute(
            (
                f"UPDATE {table_name} SET fecha_cierre_mx "
                f"= fecha_cierre - INTERVAL '{TIMEOFFSET}' "
                f"WHERE fecha_cierre IS NOT NULL;"
            )
        )
    except Exception as e:
        logger.error(f"Error setting fechas for {PIPENAME} data: {e}")


# Calculate elapsed time
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
        # Fetch tickets
        results = duckdb.sql(
            "SELECT numero_ticket, fecha_creacion_mx, fecha_cierre_mx "
            f"FROM {table_name} WHERE fecha_cierre IS NOT NULL;"
        ).fetchall()
        # Calculate elapsed time for each ticket
        for r in results:
            # Skip if no closure date
            if r[2] is None:
                continue
            # Parse dates
            start_datetime = datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S")
            end_datetime = datetime.strptime(r[2], "%Y-%m-%d %H:%M:%S")
            # Calculate elapsed time in minutes
            minutes = calculator.calculate_working_time(
                start_dt=start_datetime,
                end_dt=end_datetime,
                unit="minutes"
            )
            hours = f"{minutes / 60:.2f}"
            # Update the table
            duckdb.sql(
                f"UPDATE {table_name} SET elapsed_time_minutes = {minutes}, "
                f"elapsed_time_hours = {hours} "
                f"WHERE numero_ticket = '{r[0]}';"
            )
    except Exception as e:
        print(f"Error calculating elapsed time: {e}")
        return


def calculate_sla_met(table_name: str):
    try:
        results = duckdb.sql(
            "SELECT numero_ticket, prioridad, elapsed_time_minutes "
            f"FROM {table_name} WHERE fecha_cierre IS NOT NULL;"
        ).fetchall()

        for r in results:
            # Match the priority to get the SLA threshold
            sla_met = False
            sla_met_min = decimal.Decimal(r[2])
            match r[1]:
                case "Emergency":
                    if sla_met_min <= SLA_THRESHOLDS_MIN["Emergency"]:
                        sla_met = True
                case "Low":
                    if sla_met_min <= SLA_THRESHOLDS_MIN["Low"]:
                        sla_met = True
                case "Medium":
                    if sla_met_min <= SLA_THRESHOLDS_MIN["Medium"]:
                        sla_met = True
                case "High":
                    if sla_met_min <= SLA_THRESHOLDS_MIN["High"]:
                        sla_met = True
                case None:
                    continue

            # Set SLA met status
            if sla_met:
                duckdb.sql(
                    f"UPDATE {table_name} SET sla_met = 'Yes' "
                    f"WHERE numero_ticket = '{r[0]}';"
                )
            else:
                duckdb.sql(
                    f"UPDATE {table_name} SET sla_met = 'No' "
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
        calculate_sla_met(table_name)
    except Exception as e:
        logger.error(f"Error transforming {PIPENAME} data: {e}")


def start(pipeline_folder, db_pool, input_files, table_list: list[str]):
    for table in table_list:
        transform(table)
