"""Indicative story summary computed from the raw files.

This module exists to *check* that the generated records tell the intended story. It
re-derives a few dashboard-style figures (fill rate by segment, risk band mix, constraint
mix, median cycle time, funnel conversion, 60-day early attrition by start cohort) using
simple rules. These figures are printed for documentation and used by tests; they are
never written into the raw outputs. The governed definitions live in ta-exec-db and are
implemented in dbt.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from .config import GeneratorConfig
from .funnel import STAGES


def _latest_snapshot(snap: pl.DataFrame, as_of: dt.date) -> pl.DataFrame:
    return (
        snap.filter(pl.col("snapshot_date") <= as_of)
        .sort(["requisition_id", "snapshot_date"])
        .group_by("requisition_id", maintain_order=True)
        .agg(pl.all().last())
    )


def accepted_offers(tables: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """One row per application with an acceptance: earliest accepted date per first cycle, plus loss flags."""
    ov = tables["ats_offer_version"]
    return (
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
        .when(pl.col("days_to_toad") <= 7)
        .then(pl.lit("high_risk"))
        .when(pl.col("days_to_toad") <= 14)
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
            at_risk_positions=pl.col("openings_position").filter(pl.col("days_to_toad") <= 14).sum(),
        )
        .sort("job_family_code")
    )

    # time to fill (approval -> earliest acceptance) -------------------------------------
    ttf = acc.join(req_attrs, on="requisition_id").with_columns(
        ttf_days=(pl.col("accepted_date") - pl.col("approval_date")).dt.total_days()
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
