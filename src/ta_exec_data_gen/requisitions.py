"""Requisition master: the hiring plan turned into approved requisitions.

The demand plan is a monthly curve of positions by Target Hire Date (base level, growth,
seasonality, a hiring surge). Each month's positions are split into requisitions with a
business unit, job family, job level and seat count, then given an approval date, a
Target Offer Acceptance Date and a set of lifecycle *plans* (possible cancellation,
partial cancellation, re-baselining) that the funnel simulation applies only if the
requisition is still open when the planned day arrives.

Requisitions whose approval date falls after the as-of date do not exist yet in the ATS
and are dropped, which is what makes far-future demand thinner than near-future demand.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import polars as pl

from .config import GeneratorConfig
from .dates import DayIndex, month_starts
from .rng import RngFactory


def _in_window(day: np.ndarray, start: int, end: int) -> np.ndarray:
    return (day >= start) & (day <= end)


def build_requisition_master(cfg: GeneratorConfig, rngs: RngFactory) -> pl.DataFrame:
    rng = rngs.stream("requisitions")
    idx = DayIndex(cfg.dates.history_start)
    as_of = idx.to_day(cfg.dates.as_of)
    end_day = idx.to_day(cfg.dates.future_thd_end)

    bu_codes = [bu.code for bu in cfg.business_units]
    bu_w = np.array([bu.weight for bu in cfg.business_units])
    bu_w = bu_w / bu_w.sum()
    lvl_codes = [jl.code for jl in cfg.job_levels]
    lvl_w = np.array([jl.weight for jl in cfg.job_levels])
    lvl_w = lvl_w / lvl_w.sum()
    lvl_rank = {jl.code: jl.level_rank for jl in cfg.job_levels}
    jf_by_code = {jf.code: jf for jf in cfg.job_families}

    surge = cfg.episodes.hiring_surge
    surge_start, surge_end = idx.to_day(surge.start), idx.to_day(surge.end)
    freeze = cfg.episodes.hiring_freeze
    freeze_start, freeze_end = idx.to_day(freeze.start), idx.to_day(freeze.end)

    rows: dict[str, list] = {
        "bu_code": [],
        "jf_code": [],
        "level_code": [],
        "requested_positions": [],
        "thd_day": [],
        "thd_month_index": [],
    }

    months = month_starts(cfg.dates.history_start, cfg.dates.future_thd_end)
    for month_index, month_start in enumerate(months):
        planned = (
            cfg.demand.base_positions_per_month
            * (1.0 + cfg.demand.monthly_growth_rate) ** month_index
            * cfg.demand.seasonality[month_start.month - 1]
        )
        mid = idx.to_day(month_start) + 14
        if surge_start <= mid <= surge_end:
            planned *= surge.demand_multiplier
        target = int(rng.poisson(planned))
        # Mondays in this month are the candidate Target Hire Dates.
        first = idx.to_day(month_start)
        next_month = month_starts(month_start, month_start + dt.timedelta(days=32))[-1]
        last = min(idx.to_day(next_month) - 1, end_day)
        mondays = [
            d for d in range(first, last + 1) if idx.to_date(d).weekday() == 0 and d >= cfg.demand.min_thd_gap_days
        ]
        seats_so_far = 0
        while seats_so_far < target:
            bu = bu_codes[rng.choice(len(bu_codes), p=bu_w)]
            mix = cfg.business_unit_job_family_mix[bu]
            jf_options = list(mix)
            jf_w = np.array([mix[k] for k in jf_options]) / sum(mix.values())
            jf_code = jf_options[rng.choice(len(jf_options), p=jf_w)]
            jf = jf_by_code[jf_code]
            level = lvl_codes[rng.choice(len(lvl_codes), p=lvl_w)]
            seats = 1
            if lvl_rank[level] <= 3 and rng.random() < jf.multi_position_probability:
                # geometric-ish seat count, heavier at 2-3, capped at the family maximum
                seats = min(jf.max_positions, 2 + int(rng.geometric(0.45) - 1))
            rows["bu_code"].append(bu)
            rows["jf_code"].append(jf_code)
            rows["level_code"].append(level)
            rows["requested_positions"].append(int(seats))
            rows["thd_day"].append(int(rng.choice(mondays)))
            rows["thd_month_index"].append(month_index)
            seats_so_far += seats

    n = len(rows["thd_day"])
    thd = np.array(rows["thd_day"])
    level_codes = np.array(rows["level_code"])

    # approval lead: standard vs early annual-plan approvals
    lead_std = rng.triangular(
        cfg.demand.approval_lead_days.min,
        cfg.demand.approval_lead_days.mode,
        cfg.demand.approval_lead_days.max,
        size=n,
    )
    lead_early = rng.triangular(
        cfg.demand.early_plan_lead_days.min,
        cfg.demand.early_plan_lead_days.mode,
        cfg.demand.early_plan_lead_days.max,
        size=n,
    )
    is_early = rng.random(n) < cfg.demand.early_plan_share
    lead = np.where(is_early, lead_early, lead_std).round().astype(int)
    approval = thd - lead
    clamped = approval < 0
    approval = np.where(clamped, rng.integers(0, 21, size=n), approval)
    approval = np.minimum(approval, thd - cfg.demand.min_thd_gap_days)

    # TOAD: THD minus a level-dependent lead, never before approval + 7
    toad_lead = np.array([cfg.job_level(c).toad_lead_days for c in level_codes])
    toad = thd - toad_lead - rng.integers(0, 11, size=n)
    toad = np.maximum(toad, approval + 7)
    toad = np.minimum(toad, thd)

    keep = approval <= as_of
    order = np.lexsort((rng.random(n), thd, approval))
    order = order[keep[order]]

    df = pl.DataFrame(
        {
            "bu_code": np.array(rows["bu_code"])[order],
            "jf_code": np.array(rows["jf_code"])[order],
            "level_code": level_codes[order],
            "requested_positions": np.array(rows["requested_positions"])[order],
            "approval_day": approval[order],
            "thd_day": thd[order],
            "toad_day": toad[order],
        }
    )
    n = df.height
    approval = df["approval_day"].to_numpy()
    thd = df["thd_day"].to_numpy()
    seats = df["requested_positions"].to_numpy()

    # ---------------------------------------------------------------- lifecycle plans
    rq = cfg.requisitions
    exposed_to_freeze = _in_window(approval, freeze_start - 75, freeze_end)
    p_cancel = np.where(exposed_to_freeze, freeze.cancellation_probability, rq.base_cancellation_probability)
    has_cancel = rng.random(n) < p_cancel
    cancel_day_std = approval + rng.integers(rq.cancellation_day_range[0], rq.cancellation_day_range[1] + 1, size=n)
    cancel_day_freeze = np.maximum(approval + 5, rng.integers(freeze_start, freeze_end + 1, size=n))
    cancel_day = np.where(exposed_to_freeze, cancel_day_freeze, cancel_day_std)
    cancel_day = np.where(has_cancel, cancel_day, -1)

    has_stale = rng.random(n) < rq.stale_cancel_probability
    stale_day = thd + rng.integers(rq.stale_cancel_days_after_thd[0], rq.stale_cancel_days_after_thd[1] + 1, size=n)
    stale_day = np.where(has_stale, stale_day, -1)

    multi = seats > 1
    has_partial = multi & (rng.random(n) < rq.partial_cancel_probability)
    partial_day = approval + rng.integers(rq.partial_cancel_day_range[0], rq.partial_cancel_day_range[1] + 1, size=n)
    partial_seats = np.where(multi, rng.integers(1, np.maximum(seats, 2)), 0)
    partial_day = np.where(has_partial, partial_day, -1)
    partial_seats = np.where(has_partial, partial_seats, 0)

    has_rebase = rng.random(n) < rq.rebaseline_probability
    rebase_delay = rng.integers(rq.rebaseline_delay_days[0], rq.rebaseline_delay_days[1] + 1, size=n)
    rebase_shift = rng.integers(rq.rebaseline_shift_days[0], rq.rebaseline_shift_days[1] + 1, size=n)
    has_rebase2 = has_rebase & (rng.random(n) < rq.second_rebaseline_probability)
    rebase_delay2 = rng.integers(rq.rebaseline_delay_days[0], rq.rebaseline_delay_days[1] + 1, size=n)
    rebase_shift2 = rng.integers(rq.rebaseline_shift_days[0], rq.rebaseline_shift_days[1] + 1, size=n)

    is_surge = _in_window(approval, surge_start, surge_end)

    # ---------------------------------------------------------------- attributes and ids
    approval_year = np.array([idx.to_date(d).year for d in approval])
    seq_in_year = np.zeros(n, dtype=int)
    counters: dict[int, int] = {}
    for i, y in enumerate(approval_year):
        counters[y] = counters.get(y, 0) + 1
        seq_in_year[i] = counters[y]
    requisition_id = [f"REQ-{y}-{s:05d}" for y, s in zip(approval_year, seq_in_year, strict=True)]

    recruiter = rng.integers(1, rq.recruiter_count + 1, size=n)
    hiring_manager = rng.integers(1, rq.hiring_manager_count + 1, size=n)
    location = rng.choice(np.asarray(rq.locations, dtype=object), size=n)
    level_name = {jl.code: jl.name for jl in cfg.job_levels}
    titles = []
    for jf_code, lvl in zip(df["jf_code"].to_list(), df["level_code"].to_list(), strict=True):
        role = jf_by_code[jf_code].role_title
        titles.append(role if level_name[lvl] == "Professional" else f"{level_name[lvl]} {role}")

    return df.with_columns(
        req_idx=pl.int_range(0, n),
        requisition_id=pl.Series(requisition_id),
        requisition_title=pl.Series(titles),
        recruiter_id=pl.Series([f"REC-{r:02d}" for r in recruiter]),
        hiring_manager_id=pl.Series([f"HM-{h:04d}" for h in hiring_manager]),
        work_location=pl.Series(location.astype(str)),
        is_surge=pl.Series(is_surge),
        cancel_day=pl.Series(cancel_day),
        stale_day=pl.Series(stale_day),
        partial_day=pl.Series(partial_day),
        partial_seats=pl.Series(partial_seats),
        has_rebase=pl.Series(has_rebase),
        rebase_delay=pl.Series(rebase_delay),
        rebase_shift=pl.Series(rebase_shift),
        has_rebase2=pl.Series(has_rebase2),
        rebase_delay2=pl.Series(rebase_delay2),
        rebase_shift2=pl.Series(rebase_shift2),
        u_constraint=pl.Series(rng.random(n)),
        u_constraint2=pl.Series(rng.random(n)),
    ).select(
        "req_idx",
        "requisition_id",
        "requisition_title",
        "bu_code",
        "jf_code",
        "level_code",
        "recruiter_id",
        "hiring_manager_id",
        "work_location",
        "requested_positions",
        "approval_day",
        "thd_day",
        "toad_day",
        "is_surge",
        "cancel_day",
        "stale_day",
        "partial_day",
        "partial_seats",
        "has_rebase",
        "rebase_delay",
        "rebase_shift",
        "has_rebase2",
        "rebase_delay2",
        "rebase_shift2",
        "u_constraint",
        "u_constraint2",
    )
