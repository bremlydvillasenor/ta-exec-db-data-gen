"""Date helpers. Internally the generator works in integer day offsets from history_start."""

from __future__ import annotations

import calendar
import datetime as dt

import numpy as np
import polars as pl


class DayIndex:
    """Converts between calendar dates and integer day offsets from an origin date."""

    def __init__(self, origin: dt.date) -> None:
        self.origin = origin

    def to_day(self, date: dt.date) -> int:
        return (date - self.origin).days

    def to_date(self, day: int) -> dt.date:
        return self.origin + dt.timedelta(days=int(day))

    def expr(self, column: str | pl.Expr) -> pl.Expr:
        """Polars expression converting an integer day-offset column into a Date."""
        col = pl.col(column) if isinstance(column, str) else column
        return (pl.lit(self.origin) + pl.duration(days=col)).cast(pl.Date)


def month_ends(start: dt.date, end: dt.date) -> list[dt.date]:
    """Every calendar month-end date between start and end, inclusive."""
    out: list[dt.date] = []
    year, month = start.year, start.month
    while True:
        last = dt.date(year, month, calendar.monthrange(year, month)[1])
        if last > end:
            break
        if last >= start:
            out.append(last)
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def month_starts(start: dt.date, end: dt.date) -> list[dt.date]:
    out: list[dt.date] = []
    year, month = start.year, start.month
    while dt.date(year, month, 1) <= end:
        out.append(dt.date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def next_monday(day: int, origin: dt.date) -> int:
    """Snap a day offset forward to the next Monday (or itself if already Monday)."""
    weekday = (origin + dt.timedelta(days=int(day))).weekday()
    return int(day) + ((7 - weekday) % 7)


def snap_to_mondays(days: np.ndarray, origin: dt.date) -> np.ndarray:
    origin_weekday = origin.weekday()
    weekday = (origin_weekday + days) % 7
    return days + ((7 - weekday) % 7)
