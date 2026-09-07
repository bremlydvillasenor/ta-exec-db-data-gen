"""Raw extraction and update timestamps.

The contract separates three kinds of time:

* **business dates** (`YYYY-MM-DD`) — when something happened in the recruiting or
  employment process. Every metric is built on these.
* **`updated_at`** — when the *source record* was last modified, including a status or
  date correction. It is change metadata: exporting an unchanged row again must never
  move it.
* **`extracted_at`** — when the complete source extract was produced. Every row of every
  file in one batch carries the same value.

Both timestamps are UTC ISO 8601 (`2026-05-31T23:59:59Z`) and must satisfy
`updated_at <= extracted_at`.

The generator knows the day a record last changed (an approval, a stage exit, an offer
edit, an HR correction). The clock time inside that day is not simulated business
behaviour, so it is derived from a stable CRC of the record key instead of a random
stream: the same record always gets the same timestamp, independent of row order and of
how many draws another module made.
"""

from __future__ import annotations

import datetime as dt
import zlib
from collections.abc import Iterable

import polars as pl

from .config import GeneratorConfig
from .dates import DayIndex
from .funnel import NO_DAY


def business_cutoff(cfg: GeneratorConfig) -> dt.datetime:
    """End of the as-of day: no generated source change may be later than this."""
    return dt.datetime.combine(cfg.dates.as_of, dt.time(23, 59, 59))


def _seconds_in_day(keys: Iterable[str], lo_hour: int, hi_hour: int) -> list[int]:
    span = max((hi_hour - lo_hour) * 3600, 1)
    base = lo_hour * 3600
    return [base + (zlib.crc32(str(k).encode("utf-8")) % span) for k in keys]


class Timestamps:
    """Builds `updated_at` / `extracted_at` expressions for one generated batch."""

    def __init__(self, cfg: GeneratorConfig) -> None:
        self.cfg = cfg
        self.idx = DayIndex(cfg.dates.history_start)
        self.extracted_at = cfg.timestamps.extracted_at
        self.reference_updated_at = cfg.timestamps.reference_updated_at
        self.cutoff = business_cutoff(cfg)

    # ------------------------------------------------------------------ columns
    def extracted_at_column(self, height: int) -> pl.Series:
        return pl.Series("extracted_at", [self.extracted_at] * height, dtype=pl.Datetime("us"))

    def updated_at_from_days(self, days: Iterable[int], keys: Iterable[str]) -> pl.Series:
        """Turn integer change-day offsets plus stable record keys into `updated_at`.

        A day of `NO_DAY` means the generator has no recorded change for that row, which
        cannot happen for a row that exists; it is clamped to the record's own key day by
        the caller before this point.
        """
        day_list = [int(d) for d in days]
        secs = _seconds_in_day(keys, self.cfg.timestamps.change_hour_min, self.cfg.timestamps.change_hour_max)
        out: list[dt.datetime] = []
        for day, sec in zip(day_list, secs, strict=True):
            if day == NO_DAY:
                raise ValueError("updated_at needs a recorded change day for every row")
            stamp = dt.datetime.combine(self.idx.to_date(day), dt.time()) + dt.timedelta(seconds=sec)
            out.append(min(stamp, self.cutoff, self.extracted_at))
        return pl.Series("updated_at", out, dtype=pl.Datetime("us"))

    def stamp(self, frame: pl.DataFrame, change_day: str, key: str) -> pl.DataFrame:
        """Append `updated_at` (from a change-day column) and `extracted_at` to a frame."""
        return frame.with_columns(
            self.updated_at_from_days(frame[change_day].to_list(), frame[key].to_list()),
            self.extracted_at_column(frame.height),
        ).drop(change_day)

    def stamp_reference(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Lookup rows change only when a source label or code changes."""
        return frame.with_columns(
            pl.Series("updated_at", [self.reference_updated_at] * frame.height, dtype=pl.Datetime("us")),
            self.extracted_at_column(frame.height),
        )
