"""Indicative story summary computed from the raw files.

This module exists to *check* that the generated records tell the intended story. It
re-derives a few dashboard-style figures (fill rate by segment, risk band mix, constraint
mix, median cycle time, funnel conversion, forecast fill rate, 60-day early attrition by
start cohort) using simple rules. These figures are printed for documentation and used by
tests; they are never written into the raw outputs. The governed definitions live in
ta-exec-db and are implemented in dbt.

Where the contract states a population rule, this summary follows it, so that the numbers
here can be compared with the governed marts rather than quietly differing from them:

* ambiguous multiple-acceptance applications are **quarantined**, never collapsed
  (`fct_application` business rules), so they are outside every accepted-offer figure;
* Time to Fill runs on **non-cancelled** requisitions (EXEC-05 `population`);
* the forecast follows FCST-01..04, including the segment fallback order and the
  requisition-level cap.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from .config import GeneratorConfig
from .funnel import STAGES

# Segment grains the stage yield is trained on, coarsening in the contract's fallback order.
YIELD_FALLBACK: list[tuple[str, list[str]]] = [
    ("bu_jf_jl", ["business_unit_code", "job_family_code", "job_level_code"]),
    ("jf_jl", ["job_family_code", "job_level_code"]),
    ("jf", ["job_family_code"]),
    ("all", []),
]


def _latest_snapshot(snap: pl.DataFrame, as_of: dt.date) -> pl.DataFrame:
    return (
        snap.filter(pl.col("snapshot_date") <= as_of)
        .sort(["requisition_id", "snapshot_date"])
        .group_by("requisition_id", maintain_order=True)
        .agg(pl.all().last())
    )


def quarantined_applications(tables: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Applications carrying more than one distinct accepted offer cycle.

    The contract holds these for human review: administrative revisions of one accepted
    offer are collapsed, but a second acceptance cycle is ambiguous and must never reach
    the governed application fact. They therefore contribute no accepted-offer event, no
    Time to Fill and no filled seat here either.
    """
    return (
        tables["ats_offer_version"]
        .filter(pl.col("offer_accepted_date").is_not_null())
        .group_by("application_id")
        .agg(accepted_cycles=pl.col("offer_id").n_unique(), accepted_versions=pl.len())
        .filter(pl.col("accepted_cycles") > 1)
        .sort("application_id")
    )


def accepted_offers(tables: dict[str, pl.DataFrame], *, include_quarantined: bool = False) -> pl.DataFrame:
    """One row per application with a governed acceptance: earliest accepted date, plus loss flags.

    Quarantined applications are excluded unless `include_quarantined` is set, which exists
    only so the summary can show the size of the quarantine.
    """
    ov = tables["ats_offer_version"]
    acc = (
        ov.filter(pl.col("offer_accepted_date").is_not_null())
        .group_by("application_id", "offer_id")
        .agg(
            accepted_date=pl.col("offer_accepted_date").min(),
            lost=(pl.col("offer_rescinded_date").is_not_null() | pl.col("candidate_renege_date").is_not_null()).any(),
        )
        .sort(["application_id", "accepted_date"])
        .group_by("application_id", maintain_order=True)
        .agg(accepted_date=pl.col("accepted_date").first(), lost=pl.col("lost").last(), cycles=pl.len())
        .join(tables["ats_application"].select("application_id", "requisition_id"), on="application_id")
    )
    if include_quarantined:
        return acc
    return acc.join(quarantined_applications(tables).select("application_id"), on="application_id", how="anti")


def stage_yields(tables: dict[str, pl.DataFrame], latest: pl.DataFrame, cfg: GeneratorConfig) -> pl.DataFrame:
    """FCST-01 stage-to-active-fill yield per segment and stage, with the documented fallback.

    The training target is the *active fill* - an acceptance that was not later rescinded or
    reneged - so the forecast stays on the same definition as filled positions. Only
    applications with a final outcome train the model; active candidates are what the model
    is later asked to predict. A segment grain is used only when it has at least
    `story.forecast_min_segment_observations` observations, otherwise the next coarser grain
    in `YIELD_FALLBACK` applies.
    """
    min_obs = cfg.story.forecast_min_segment_observations
    acc = accepted_offers(tables)
    active_fill = acc.filter(~pl.col("lost")).select("application_id").with_columns(is_active_fill=pl.lit(True))
    segments = latest.select("requisition_id", "business_unit_code", "job_family_code", "job_level_code")
    entered = (
        tables["ats_stage_history"]
        .select("application_id", "stage_code")
        .unique()
        .join(
            tables["ats_application"].select("application_id", "requisition_id", "application_status"),
            on="application_id",
        )
        .filter(pl.col("application_status") != "active")
        .join(segments, on="requisition_id")
        .join(active_fill, on="application_id", how="left")
        .with_columns(pl.col("is_active_fill").fill_null(False))
    )
    grain = entered.select("business_unit_code", "job_family_code", "job_level_code", "stage_code").unique()
    for name, keys in YIELD_FALLBACK:
        level = entered.group_by([*keys, "stage_code"]).agg(
            **{
                f"n_{name}": pl.len(),
                f"fills_{name}": pl.col("is_active_fill").sum(),
            }
        )
        grain = grain.join(level, on=[*keys, "stage_code"], how="left")
    applied_level = pl.lit(None, dtype=pl.Utf8)
    applied_yield = pl.lit(None, dtype=pl.Float64)
    applied_obs = pl.lit(None, dtype=pl.Int64)
    for name, _ in reversed(YIELD_FALLBACK):
        usable = pl.col(f"n_{name}").fill_null(0) >= min_obs
        applied_level = pl.when(usable).then(pl.lit(name)).otherwise(applied_level)
        applied_yield = pl.when(usable).then(pl.col(f"fills_{name}") / pl.col(f"n_{name}")).otherwise(applied_yield)
        applied_obs = pl.when(usable).then(pl.col(f"n_{name}")).otherwise(applied_obs)
    return grain.with_columns(
        yield_segment_level=applied_level,
        applied_yield=applied_yield,
        applied_observations=applied_obs,
    ).sort(["business_unit_code", "job_family_code", "job_level_code", "stage_code"])


def expected_pipeline_fills(
    tables: dict[str, pl.DataFrame], latest: pl.DataFrame, yields: pl.DataFrame
) -> pl.DataFrame:
    """FCST-02: active-pipeline yield summed per requisition and capped at its remaining openings."""
    segments = latest.select(
        "requisition_id", "business_unit_code", "job_family_code", "job_level_code", "requisition_status",
        "openings_position",
    )
    active = (
        tables["ats_application"]
        .filter(pl.col("application_status") == "active")
        .select("application_id", "requisition_id", stage_code=pl.col("current_stage_code"))
        .join(segments, on="requisition_id")
        .filter(pl.col("requisition_status") == "open")
        .join(
            yields.select("business_unit_code", "job_family_code", "job_level_code", "stage_code", "applied_yield"),
            on=["business_unit_code", "job_family_code", "job_level_code", "stage_code"],
            how="left",
        )
        .with_columns(pl.col("applied_yield").fill_null(0.0))
    )
    raw = active.group_by("requisition_id").agg(
        active_candidates=pl.len(), uncapped_expected_fills=pl.col("applied_yield").sum()
    )
    return (
        segments.filter(pl.col("requisition_status") == "open")
        .join(raw, on="requisition_id", how="left")
        .with_columns(
            pl.col("active_candidates").fill_null(0),
            pl.col("uncapped_expected_fills").fill_null(0.0),
        )
        .with_columns(
            expected_pipeline_fills=pl.min_horizontal("uncapped_expected_fills", pl.col("openings_position"))
        )
        .select("requisition_id", "active_candidates", "uncapped_expected_fills", "expected_pipeline_fills")
    )


def _risk_band_by_thd_window(open_reqs: pl.DataFrame, as_of: dt.date, cfg: GeneratorConfig) -> pl.DataFrame:
    """Risk band mix under the THD period selections the dashboard is likely to offer."""
    windows = [
        ("2026-01..2026-05 (wireframe default)", dt.date(2026, 1, 1), dt.date(2026, 5, 31)),
        ("2026-06..2027-05 (future THD)", dt.date(2026, 6, 1), cfg.dates.future_thd_end),
        ("all THD (no period filter)", cfg.dates.history_start, cfg.dates.future_thd_end),
    ]
    bands = ["missed", "high_risk", "medium_risk", "on_track"]
    rows = []
    for label, lo, hi in windows:
        sel = open_reqs.filter((pl.col("target_hire_date") >= lo) & (pl.col("target_hire_date") <= hi))
        by_band = dict(sel.group_by("risk_band").agg(pl.col("openings_position").sum()).iter_rows())
        total = sel["openings_position"].sum()
        at_risk = total - by_band.get("on_track", 0)
        rows.append(
            {
                "thd_window": label,
                **{band: int(by_band.get(band, 0)) for band in bands},
                "open_positions": int(total),
                "at_risk_share": round(at_risk / total, 3) if total else None,
            }
        )
    return pl.DataFrame(rows)


def _forecast_summary(forecast: pl.DataFrame, as_of: dt.date, cfg: GeneratorConfig) -> pl.DataFrame:
    """FCST-03/04 against EXEC-01, for the whole plan and for the delivered period."""
    windows = [
        ("all THD", cfg.dates.history_start, cfg.dates.future_thd_end),
        ("2026-01..2026-05 (wireframe default)", dt.date(2026, 1, 1), dt.date(2026, 5, 31)),
        ("THD on or before as-of", cfg.dates.history_start, as_of),
        ("THD after as-of", as_of + dt.timedelta(days=1), cfg.dates.future_thd_end),
    ]
    rows = []
    for label, lo, hi in windows:
        sel = forecast.filter((pl.col("target_hire_date") >= lo) & (pl.col("target_hire_date") <= hi))
        requested = sel["requested_positions"].sum()
        filled = sel["filled"].sum()
        expected = sel["expected_pipeline_fills"].sum()
        rows.append(
            {
                "thd_window": label,
                "requested_positions": int(requested),
                "filled_positions": int(filled),
                "expected_pipeline_fills": round(float(expected), 1),
                "forecast_filled_positions": round(float(filled + expected), 1),
                "fill_rate": round(filled / requested, 3) if requested else None,
                "forecast_fill_rate": round((filled + expected) / requested, 3) if requested else None,
                "fill_rate_target": cfg.story.fill_rate_target,
            }
        )
    return pl.DataFrame(rows)


def summarise(tables: dict[str, pl.DataFrame], cfg: GeneratorConfig) -> dict[str, pl.DataFrame]:
    as_of = cfg.dates.as_of
    snap = tables["ats_requisition_snapshot"]
    latest = _latest_snapshot(snap, as_of)
    acc = accepted_offers(tables)
    req_attrs = latest.select(
        "requisition_id",
        "business_unit_code",
        "job_family_code",
        "job_level_code",
        "approval_date",
        "target_hire_date",
        "requisition_status",
    )
    out: dict[str, pl.DataFrame] = {}

    # demand and fill by THD year-month and business unit ----------------------------
    demand = latest.filter(pl.col("requisition_status") != "cancelled").with_columns(
        thd_month=pl.col("target_hire_date").dt.strftime("%Y-%m"),
        filled=pl.col("requested_positions") - pl.col("openings_position"),
    )
    out["fill_by_business_unit"] = (
        demand.filter(pl.col("target_hire_date") <= as_of)
        .group_by("business_unit_code")
        .agg(
            requested=pl.col("requested_positions").sum(),
            filled=pl.col("filled").sum(),
            open=pl.col("openings_position").sum(),
        )
        .with_columns(fill_rate=(pl.col("filled") / pl.col("requested")).round(3))
        .sort("business_unit_code")
    )
    out["fill_by_job_family"] = (
        demand.filter(pl.col("target_hire_date") <= as_of)
        .group_by("job_family_code")
        .agg(
            requested=pl.col("requested_positions").sum(),
            filled=pl.col("filled").sum(),
            open=pl.col("openings_position").sum(),
        )
        .with_columns(fill_rate=(pl.col("filled") / pl.col("requested")).round(3))
        .sort("job_family_code")
    )
    out["demand_by_thd_month"] = (
        demand.group_by("thd_month")
        .agg(
            requested=pl.col("requested_positions").sum(),
            filled=pl.col("filled").sum(),
            open=pl.col("openings_position").sum(),
        )
        .with_columns(fill_rate=(pl.col("filled") / pl.col("requested")).round(3))
        .sort("thd_month")
    )

    # risk bands and constraints on open requisitions -----------------------------------
    open_reqs = latest.filter(
        (pl.col("requisition_status") == "open") & (pl.col("openings_position") > 0)
    ).with_columns(days_to_toad=(pl.col("target_offer_acceptance_date") - pl.lit(as_of)).dt.total_days())
    open_reqs = open_reqs.with_columns(
        risk_band=pl.when(pl.col("days_to_toad") < 0)
        .then(pl.lit("missed"))
        .when(pl.col("days_to_toad") <= cfg.story.risk_high_max_days)
        .then(pl.lit("high_risk"))
        .when(pl.col("days_to_toad") <= cfg.story.risk_medium_max_days)
        .then(pl.lit("medium_risk"))
        .otherwise(pl.lit("on_track"))
    )
    out["open_positions_by_risk_band"] = (
        open_reqs.group_by("risk_band")
        .agg(requisitions=pl.len(), open_positions=pl.col("openings_position").sum())
        .sort("risk_band")
    )
    out["open_positions_by_constraint"] = (
        open_reqs.group_by("primary_hiring_constraint")
        .agg(open_positions=pl.col("openings_position").sum())
        .sort("open_positions", descending=True)
    )
    out["constraint_by_risk"] = (
        open_reqs.group_by("risk_band", "primary_hiring_constraint")
        .agg(open_positions=pl.col("openings_position").sum())
        .sort(["risk_band", "open_positions"], descending=[False, True])
    )
    out["risk_by_job_family"] = (
        open_reqs.group_by("job_family_code")
        .agg(
            open_positions=pl.col("openings_position").sum(),
            at_risk_positions=pl.col("openings_position")
            .filter(pl.col("days_to_toad") <= cfg.story.risk_medium_max_days)
            .sum(),
        )
        .sort("job_family_code")
    )
    # What the risk visual shows under a THD period selection. TOAD is always on or before
    # THD, so a selection that ends on the as-of date can only ever contain missed
    # positions: every band except Missed needs THD after the as-of date. This is a
    # property of the metric, not of the data, and it is surfaced here so the dashboard's
    # default THD slicer can be chosen with it in mind.
    out["risk_band_by_thd_window"] = _risk_band_by_thd_window(open_reqs, as_of, cfg)

    # time to fill (approval -> earliest acceptance) -------------------------------------
    # EXEC-05 population: every accepted-offer event on a NON-CANCELLED requisition, with
    # quarantined applications already removed by accepted_offers(). Offers later rescinded
    # or reneged stay in: Time to Fill measures the cycle TA actually completed.
    non_cancelled = latest.filter(pl.col("requisition_status") != "cancelled").select("requisition_id")
    ttf = (
        acc.join(non_cancelled, on="requisition_id", how="semi")
        .join(req_attrs, on="requisition_id")
        .with_columns(ttf_days=(pl.col("accepted_date") - pl.col("approval_date")).dt.total_days())
    )
    out["time_to_fill_by_job_family"] = (
        ttf.group_by("job_family_code")
        .agg(
            accepted_offers=pl.len(),
            lost_after_acceptance=pl.col("lost").sum(),
            median_ttf_days=pl.col("ttf_days").median(),
        )
        .sort("job_family_code")
    )
    out["time_to_fill_overall"] = ttf.select(
        accepted_offers=pl.len(),
        median_ttf_days=pl.col("ttf_days").median(),
        lost_after_acceptance=pl.col("lost").sum(),
    )
    # How the source population narrows to the governed one. Each step is a contract rule,
    # so the two ends of this table are both correct answers to different questions.
    source_acc = accepted_offers(tables, include_quarantined=True)
    quarantined = quarantined_applications(tables)
    cancelled_drop = acc.join(non_cancelled, on="requisition_id", how="anti")
    out["time_to_fill_population"] = pl.DataFrame(
        {
            "step": [
                "applications with an acceptance in the source",
                "less quarantined (more than one acceptance cycle)",
                "less acceptances on cancelled requisitions",
                "governed EXEC-05 population",
            ],
            "applications": [
                source_acc.height,
                -quarantined.height,
                -cancelled_drop.height,
                ttf.height,
            ],
            "median_ttf_days": [
                source_acc.join(req_attrs, on="requisition_id")
                .select((pl.col("accepted_date") - pl.col("approval_date")).dt.total_days().median())
                .item(),
                None,
                None,
                ttf["ttf_days"].median(),
            ],
        }
    )

    # forecast: FCST-01 stage yield -> FCST-02 expected fills -> FCST-03/04 ------------------
    yields = stage_yields(tables, latest, cfg)
    fills = expected_pipeline_fills(tables, latest, yields)
    out["stage_yield_by_fallback_level"] = (
        yields.group_by("stage_code", "yield_segment_level")
        .agg(
            segments=pl.len(),
            min_yield=pl.col("applied_yield").min().round(3),
            median_yield=pl.col("applied_yield").median().round(3),
            max_yield=pl.col("applied_yield").max().round(3),
        )
        .with_columns(stage_order=pl.col("stage_code").replace_strict({c: i for i, c in enumerate(STAGES)}))
        .sort(["stage_order", "yield_segment_level"])
        .drop("stage_order")
    )
    forecast = (
        demand.join(fills, on="requisition_id", how="left")
        .with_columns(pl.col("expected_pipeline_fills").fill_null(0.0))
        .with_columns(forecast_filled=pl.col("filled") + pl.col("expected_pipeline_fills"))
    )
    out["forecast_by_thd_month"] = (
        forecast.group_by("thd_month")
        .agg(
            requested=pl.col("requested_positions").sum(),
            filled=pl.col("filled").sum(),
            expected_pipeline_fills=pl.col("expected_pipeline_fills").sum().round(1),
            forecast_filled=pl.col("forecast_filled").sum().round(1),
        )
        .with_columns(
            fill_rate=(pl.col("filled") / pl.col("requested")).round(3),
            forecast_fill_rate=(pl.col("forecast_filled") / pl.col("requested")).round(3),
        )
        .sort("thd_month")
    )
    out["forecast_summary"] = _forecast_summary(forecast, as_of, cfg)

    # funnel: active snapshot, completed conversion, completed median days ------------------
    stg = tables["ats_stage_history"].join(
        tables["ats_application"].select("application_id", "application_status"), on="application_id"
    )
    stg = stg.sort(["application_id", "stage_sequence"]).with_columns(
        next_stage=pl.col("stage_code").shift(-1).over("application_id")
    )
    accepted_ids = acc.select("application_id").with_columns(accepted=pl.lit(True))
    stg = stg.join(accepted_ids, on="application_id", how="left").with_columns(pl.col("accepted").fill_null(False))
    completed = stg.filter(pl.col("stage_exited_date").is_not_null()).with_columns(
        advanced=pl.when(pl.col("stage_code") == "offer")
        .then(pl.col("accepted"))
        .otherwise(pl.col("next_stage").is_not_null()),
        days=(pl.col("stage_exited_date") - pl.col("stage_entered_date")).dt.total_days(),
    )
    order = {c: i for i, c in enumerate(STAGES)}
    out["funnel_by_stage"] = (
        completed.group_by("stage_code")
        .agg(completed=pl.len(), advanced=pl.col("advanced").sum(), median_days=pl.col("days").median())
        .with_columns(conversion=(pl.col("advanced") / pl.col("completed")).round(3))
        .join(
            stg.filter(pl.col("stage_exited_date").is_null()).group_by("stage_code").agg(active_now=pl.len()),
            on="stage_code",
            how="left",
        )
        .with_columns(pl.col("active_now").fill_null(0), stage_order=pl.col("stage_code").replace_strict(order))
        .sort("stage_order")
        .drop("stage_order")
    )
    out["interview_conversion_by_job_family"] = (
        completed.filter(pl.col("stage_code") == "interview")
        .join(tables["ats_application"].select("application_id", "requisition_id"), on="application_id")
        .join(req_attrs.select("requisition_id", "job_family_code"), on="requisition_id")
        .group_by("job_family_code")
        .agg(completed=pl.len(), conversion=pl.col("advanced").mean().round(3), median_days=pl.col("days").median())
        .sort("job_family_code")
    )

    # 60-day early attrition by start cohort (fully matured months only) -----------------------
    hr = tables["hr_worker_event"]
    hires = (
        hr.filter(pl.col("event_type") == "hire")
        .group_by("worker_id", "application_id", "requisition_id")
        .agg(start=pl.col("event_date").min())
    )
    terms = hr.filter(pl.col("event_type") == "termination").group_by("worker_id").agg(term=pl.col("event_date").min())
    quality = (
        hires.join(terms, on="worker_id", how="left")
        .join(req_attrs, on="requisition_id")
        .with_columns(
            cohort=pl.col("start").dt.strftime("%Y-%m"),
            tenure=(pl.col("term") - pl.col("start")).dt.total_days(),
            month_end=pl.col("start").dt.month_end(),
        )
        .with_columns(
            early=(pl.col("tenure").is_not_null() & (pl.col("tenure") <= 60)),
            matured=(pl.col("month_end") + pl.duration(days=60)) <= pl.lit(as_of),
        )
        .join(acc.select("application_id", "accepted_date"), on="application_id", how="left")
        .with_columns(ttf_days=(pl.col("accepted_date") - pl.col("approval_date")).dt.total_days())
    )
    out["early_attrition_by_cohort"] = (
        quality.filter(pl.col("matured"))
        .group_by("cohort")
        .agg(hires=pl.len(), early_exits=pl.col("early").sum(), median_ttf_days=pl.col("ttf_days").median())
        .with_columns(early_attrition_rate=(pl.col("early_exits") / pl.col("hires")).round(3))
        .sort("cohort")
    )
    out["early_attrition_by_business_unit"] = (
        quality.filter(pl.col("matured"))
        .group_by("business_unit_code")
        .agg(hires=pl.len(), early_exits=pl.col("early").sum())
        .with_columns(early_attrition_rate=(pl.col("early_exits") / pl.col("hires")).round(3))
        .sort("business_unit_code")
    )

    # offer versions and source quirks ----------------------------------------------------------
    ov = tables["ats_offer_version"]
    multi = (
        ov.filter(pl.col("offer_accepted_date").is_not_null())
        .group_by("application_id")
        .agg(accepted_versions=pl.len(), cycles=pl.col("offer_id").n_unique())
    )
    out["offer_version_profile"] = pl.DataFrame(
        {
            "measure": [
                "applications_with_offer",
                "offer_versions",
                "applications_with_multiple_accepted_versions",
                "applications_with_two_accepted_cycles (quarantine candidates)",
                "hire_event_rows",
                "termination_event_rows",
                "snapshot_rows",
                "requisitions",
            ],
            "value": [
                ov["application_id"].n_unique(),
                ov.height,
                multi.filter(pl.col("accepted_versions") > 1).height,
                multi.filter(pl.col("cycles") > 1).height,
                hr.filter(pl.col("event_type") == "hire").height,
                hr.filter(pl.col("event_type") == "termination").height,
                snap.height,
                latest.height,
            ],
        }
    )
    out["requisition_status_as_of"] = (
        latest.group_by("requisition_status")
        .agg(requisitions=pl.len(), open_positions=pl.col("openings_position").sum())
        .sort("requisition_status")
    )
    out["application_status_as_of"] = (
        tables["ats_application"].group_by("application_status").len().sort("application_status")
    )
    return out


def format_summary(summary: dict[str, pl.DataFrame]) -> str:
    parts = []
    with pl.Config(
        tbl_rows=60,
        tbl_cols=20,
        tbl_width_chars=140,
        tbl_hide_dataframe_shape=True,
        tbl_hide_column_data_types=True,
        tbl_formatting="ASCII_MARKDOWN",
    ):
        for name, frame in summary.items():
            parts.append(f"## {name}\n\n{frame}\n")
    return "\n".join(parts)
