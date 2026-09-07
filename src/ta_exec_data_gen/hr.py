"""HR system worker events: actual employee starts and terminations.

The HR extract is event-shaped: one `start` row per person who actually started, and one
`termination` row when they left. Early exits (within the first weeks) follow the job
family, level and story episode (rushed surge hiring leaves sooner); later exits follow a
flat annual hazard. Only a termination carries a reason, as the contract requires.

Realistic noise is added on purpose: a small share of start events are re-sent by the
integration (exact duplicates with a new event id), and a small share of terminations
carry a second, later-dated termination row that dbt must resolve to the earliest
termination after the start.

`event_changed_day` is when the HR record was created or corrected, which is what the
row's `updated_at` is built from. It is deliberately later than `event_date`: payroll
records a start or an exit after it happened.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from .config import GeneratorConfig
from .dates import DayIndex
from .funnel import NO_DAY
from .rng import RngFactory


def build_worker_events(
    apps: pl.DataFrame, master: pl.DataFrame, cfg: GeneratorConfig, rngs: RngFactory
) -> pl.DataFrame:
    rng = rngs.stream("hr")
    hc = cfg.hr
    idx = DayIndex(cfg.dates.history_start)
    as_of = idx.to_day(cfg.dates.as_of)
    surge = cfg.episodes.hiring_surge
    jf_rate = {jf.code: jf.early_attrition_rate for jf in cfg.job_families}
    lvl_mult = {jl.code: jl.attrition_multiplier for jl in cfg.job_levels}

    hires = (
        apps.filter(pl.col("start_day") != NO_DAY)
        .join(master.select("req_idx", "jf_code", "level_code", "is_surge"), on="req_idx", how="left")
        .sort(["start_day", "app_idx"])
        .with_row_index("hire_seq")
    )
    n = hires.height
    start = hires["start_day"].to_numpy()
    p_early = np.array([jf_rate[c] for c in hires["jf_code"].to_list()])
    p_early = p_early * np.array([lvl_mult[c] for c in hires["level_code"].to_list()])
    p_early = np.where(hires["is_surge"].to_numpy(), p_early * surge.early_attrition_multiplier, p_early)
    p_early = np.minimum(p_early, 0.6)

    u = rng.random(n)
    is_early = u < p_early
    lo, hi = hc.early_tenure_days
    early_tenure = lo + np.floor(rng.beta(1.3, 1.4, n) * (hi - lo + 1)).astype(int)
    early_tenure = np.clip(early_tenure, lo, hi)
    late_tenure = hi + 1 + np.floor(rng.exponential(365.0 / max(hc.late_attrition_annual_rate, 1e-6), n)).astype(int)
    tenure = np.where(is_early, early_tenure, late_tenure)
    term_day = start + tenure
    has_term = term_day <= as_of
    reasons_early = hc.termination_reasons.early
    reasons_late = hc.termination_reasons.late
    reason = np.where(
        is_early,
        rng.choice(np.asarray(reasons_early, dtype=object), size=n),
        rng.choice(np.asarray(reasons_late, dtype=object), size=n),
    )

    worker_id = [f"W-{100001 + i}" for i in range(n)]
    hires = hires.with_columns(worker_id=pl.Series(worker_id))

    hire_events = hires.select(
        "app_idx",
        "req_idx",
        "worker_id",
        event_type=pl.lit("start"),
        event_day=pl.col("start_day"),
        termination_reason=pl.lit(None, dtype=pl.Utf8),
        event_changed_day=pl.col("start_day") + pl.Series(rng.integers(0, 4, n)),
        is_duplicate=pl.lit(False),
    )
    term_frame = hires.with_columns(
        term_day=pl.Series(term_day),
        has_term=pl.Series(has_term),
        reason=pl.Series(reason.astype(str)),
    ).filter(pl.col("has_term"))
    term_events = term_frame.select(
        "app_idx",
        "req_idx",
        "worker_id",
        event_type=pl.lit("termination"),
        event_day=pl.col("term_day"),
        termination_reason=pl.col("reason"),
        event_changed_day=pl.col("term_day") + pl.Series(rng.integers(0, 6, term_frame.height)),
        is_duplicate=pl.lit(False),
    )

    # integration noise -----------------------------------------------------------
    dup_hires = hire_events.filter(pl.Series(rng.random(hire_events.height) < hc.duplicate_hire_event_share))
    dup_hires = dup_hires.with_columns(
        event_changed_day=pl.col("event_changed_day") + pl.Series(rng.integers(1, 15, dup_hires.height)),
        is_duplicate=pl.lit(True),
    )
    dup_terms = term_events.filter(pl.Series(rng.random(term_events.height) < hc.duplicate_termination_share))
    shift = rng.integers(
        hc.duplicate_termination_shift_days[0], hc.duplicate_termination_shift_days[1] + 1, dup_terms.height
    )
    dup_terms = dup_terms.with_columns(
        event_day=pl.col("event_day") + pl.Series(shift),
        event_changed_day=pl.col("event_changed_day") + pl.Series(shift) + 1,
        is_duplicate=pl.lit(True),
    )

    events = pl.concat([hire_events, term_events, dup_hires, dup_terms]).with_columns(
        event_day=pl.min_horizontal(pl.col("event_day"), pl.lit(as_of)),
        event_changed_day=pl.min_horizontal(pl.col("event_changed_day"), pl.lit(as_of)),
    )
    return events.sort(["event_day", "worker_id", "event_type", "event_changed_day"]).with_row_index("event_seq")
