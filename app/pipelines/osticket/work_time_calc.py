from datetime import datetime, time, timedelta, date
from typing import List, Tuple


class WorkingTimeCalculator:
    """
    A class to calculate elapsed working time between two datetimes
    based on defined working shifts.

    Attributes:
        working_shifts (List[Tuple[time, time]]): List of working shifts as
        (start_time, end_time) pairs weekends (List[int]): List of weekday
        numbers that are considered weekends (0=Monday, 6=Sunday)
        holidays (List[date]): List of dates that are considered holidays
    """

    def __init__(
        self,
        working_shifts: List[Tuple[time, time]],
        holidays: List[date],
        weekends: List[int] = [6, 7]  # Default: Saturday and Sunday
    ):
        """
        Initialize the WorkingTimeCalculator with working shifts,
        weekends, and holidays.

        Args:
            working_shifts: List of (start_time, end_time) tuples for each 
            working shift
            weekends: List of weekday numbers (0-6) that are considered 
            non-working days
            holidays: List of dates that are considered non-working days
        """
        # Default: 9am-5pm
        self.working_shifts = working_shifts or [(time(9, 0), time(17, 0))]
        self.weekends = weekends
        self.holidays = holidays or []

    def is_working_day(self, dt: datetime) -> bool:
        """
        Check if a given datetime falls on a working day 
        (not weekend or holiday).

        Args:
            dt: Datetime to check

        Returns:
            bool: True if it's a working day, False otherwise
        """
        if dt.weekday() in self.weekends:
            return False
        if self.holidays and dt.date() in self.holidays:
            return False
        return True

    def is_within_working_hours(self, dt: datetime) -> bool:
        """
        Check if a given datetime falls within any working shift.

        Args:
            dt: Datetime to check

        Returns:
            bool: True if within working hours, False otherwise
        """
        if not self.is_working_day(dt):
            return False

        current_time = dt.time()
        for shift_start, shift_end in self.working_shifts:
            if shift_start <= current_time < shift_end:
                return True
        return False

    def get_next_working_period_start(self, dt: datetime) -> datetime:
        """
        Get the next datetime when working period starts after the given 
        datetime.

        Args:
            dt: Reference datetime

        Returns:
            datetime: Next working period start datetime
        """
        # Check same day
        for shift_start, shift_end in self.working_shifts:
            if dt.time() < shift_start and self.is_working_day(dt):
                return datetime.combine(dt.date(), shift_start)

        # Move to next day
        next_day = dt + timedelta(days=1)
        while True:
            if self.is_working_day(next_day):
                return datetime.combine(
                    next_day.date(), self.working_shifts[0][0]
                )
            next_day += timedelta(days=1)

    def get_previous_working_period_end(self, dt: datetime) -> datetime:
        """
        Get the previous datetime when working period ended before the given 
        datetime.

        Args:
            dt: Reference datetime

        Returns:
            datetime: Previous working period end datetime
        """
        # Check same day
        for shift_start, shift_end in reversed(self.working_shifts):
            if dt.time() > shift_end and self.is_working_day(dt):
                return datetime.combine(dt.date(), shift_end)

        # Move to previous day
        prev_day = dt - timedelta(days=1)
        while True:
            if self.is_working_day(prev_day):
                return datetime.combine(
                    prev_day.date(),
                    self.working_shifts[-1][1]
                )
            prev_day -= timedelta(days=1)

    def calculate_working_time(
        self,
        start_dt: datetime,
        end_dt: datetime,
        unit: str = "hours"
    ) -> float:
        """
        Calculate the elapsed working time between two datetimes.

        Args:
            start_dt: Start datetime
            end_dt: End datetime
            unit: Unit of time to return ('seconds', 'minutes', or 'hours')

        Returns:
            float: Elapsed working time in the specified unit

        Raises:
            ValueError: If end_dt is before start_dt or if unit is invalid
        """
        if end_dt < start_dt:
            raise ValueError("End datetime must be after start datetime")

        valid_units = ["seconds", "minutes", "hours"]
        if unit not in valid_units:
            raise ValueError(f"Unit must be one of {valid_units}")

        total_seconds = 0.0
        current_dt = start_dt

        # Adjust start time if it's outside working hours
        if not self.is_within_working_hours(current_dt):
            current_dt = self.get_next_working_period_start(current_dt)
            if current_dt >= end_dt:
                return 0.0

        while current_dt < end_dt:
            # Find the current working shift
            current_shift = None
            for shift_start, shift_end in self.working_shifts:
                shift_start_dt = datetime.combine(
                    current_dt.date(), shift_start
                )
                shift_end_dt = datetime.combine(
                    current_dt.date(), shift_end
                )
                if shift_start_dt <= current_dt < shift_end_dt:
                    current_shift = (shift_start_dt, shift_end_dt)
                    break

            if not current_shift:
                # Move to next working period
                current_dt = self.get_next_working_period_start(current_dt)
                if current_dt >= end_dt:
                    break
                continue

            shift_end = current_shift[1]
            segment_end = min(shift_end, end_dt)

            # Add time from current_dt to segment_end
            total_seconds += (segment_end - current_dt).total_seconds()
            current_dt = segment_end

            # If we reached shift end, move to next working period
            if current_dt >= shift_end:
                current_dt = self.get_next_working_period_start(current_dt)

        # Convert to requested unit
        if unit == "seconds":
            return total_seconds
        elif unit == "minutes":
            return total_seconds / 60
        elif unit == "hours":
            return total_seconds / 3600
        # Fallback return to satisfy all code paths (should not be reached)
        return 0.0
