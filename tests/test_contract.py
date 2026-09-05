"""Alignment with the ta-exec-db contract: vocabulary, columns, boundaries, no derived fields."""

import datetime as dt

import polars as pl

from ta_exec_data_gen.pipeline import OUTPUT_TABLES

STAGE_ORDER = ["review", "screen", "assessment", "interview", "offer"]
CONSTRAINTS = [
    "qualified_candidates",
    "hiring_manager_delay",
    "compensation",
    "interview_capacity",
    "niche_skills",
    "candidate_availability",
    "no_material_constraint",
]
APPLICATION_STATUSES = {
    "active",
    "rejected",
    "withdrawn",
    "offer_declined",
    "offer_withdrawn",
    "offer_accepted",
    "offer_rescinded",
    "candidate_renege",
}
EXPECTED_COLUMNS = {
    "ats_business_unit": ["business_unit_code", "business_unit_name", "sort_order", "is_active"],
    "ats_job_family": ["job_family_code", "job_family_name", "sort_order", "is_active"],
    "ats_job_level": ["job_level_code", "job_level_name", "level_rank", "is_active"],
    "ats_requisition_snapshot": [
        "snapshot_date",
        "requisition_id",
        "requisition_title",
        "business_unit_code",
        "job_family_code",
        "job_level_code",
        "work_location",
        "hiring_manager_id",
        "recruiter_id",
        "requisition_status",
        "approval_date",
        "target_hire_date",
        "target_offer_acceptance_date",
        "requested_positions",
        "openings_position",
        "cancelled_positions",
        "primary_hiring_constraint",
    ],
    "ats_application": [
        "application_id",
        "candidate_id",
        "requisition_id",
        "application_date",
        "source_channel",
        "application_status",
        "status_date",
        "current_stage_code",
        "disposition_reason",
    ],
    "ats_stage_history": [
        "stage_history_id",
        "application_id",
        "stage_code",
        "stage_sequence",
        "stage_entered_date",
        "stage_exited_date",
    ],
    "ats_offer_version": [
        "offer_version_id",
        "offer_id",
        "application_id",
        "requisition_id",
        "offer_cycle_number",
        "offer_version_number",
        "version_reason",
        "offer_status",
        "is_current_version",
        "offer_extended_date",
        "offer_accepted_date",
        "offer_declined_date",
        "offer_withdrawn_date",
        "offer_rescinded_date",
        "candidate_renege_date",
        "proposed_start_date",
        "base_salary",
        "currency",
    ],
    "hr_worker_event": [
        "worker_event_id",
        "worker_id",
        "candidate_id",
        "application_id",
        "requisition_id",
        "event_type",
        "event_date",
        "event_reason",
        "record_created_date",
    ],
}
DERIVED_MARKERS = [
    "risk_band",
    "days_to_toad",
    "is_active_fill",
    "is_offer_accepted",
    "fill_rate",
    "yield",
    "cohort",
    "matured",
    "time_to_fill",
    "days_in_stage",
    "advanced_to",
    "is_started",
    "post_acceptance_outcome",
    "filled_positions",
    "attrition",
    "forecast",
]


def test_tables_and_columns(tables_small):
    assert list(tables_small) == OUTPUT_TABLES
    for name, cols in EXPECTED_COLUMNS.items():
        assert tables_small[name].columns == cols, name


def test_no_derived_analytics_columns(tables_small):
    for name, frame in tables_small.items():
        for col in frame.columns:
            assert not any(marker in col for marker in DERIVED_MARKERS), f"{name}.{col}"


def test_vocabularies_match_contract(tables_small, cfg_small):
    stg = tables_small["ats_stage_history"]
    assert set(stg["stage_code"].unique()) <= set(STAGE_ORDER)
    seq = stg.select("stage_code", "stage_sequence").unique()
    assert all(STAGE_ORDER[s - 1] == c for c, s in seq.iter_rows())
    snap = tables_small["ats_requisition_snapshot"]
    assert set(snap["primary_hiring_constraint"].unique()) <= set(CONSTRAINTS)
    assert set(snap["requisition_status"].unique()) == {"open", "filled", "cancelled"}
    assert set(tables_small["ats_application"]["application_status"].unique()) == APPLICATION_STATUSES
    assert cfg_small.hiring_constraints == CONSTRAINTS


def test_reporting_boundaries(tables_medium, cfg_medium):
    as_of, start, end = cfg_medium.dates.as_of, cfg_medium.dates.history_start, cfg_medium.dates.future_thd_end
    snap = tables_medium["ats_requisition_snapshot"]
    assert snap["target_hire_date"].max() <= end
    assert snap["target_hire_date"].max() > as_of, "future demand must exist"
    assert snap["snapshot_date"].max() == as_of
    assert snap["approval_date"].min() >= start
    for name, cols in {
        "ats_application": ["application_date", "status_date"],
        "ats_stage_history": ["stage_entered_date", "stage_exited_date"],
        "ats_offer_version": [
            "offer_extended_date",
            "offer_accepted_date",
            "offer_declined_date",
            "offer_withdrawn_date",
            "offer_rescinded_date",
            "candidate_renege_date",
        ],
        "hr_worker_event": ["event_date", "record_created_date"],
    }.items():
        for col in cols:
            mx = tables_medium[name][col].max()
            assert mx is None or mx <= as_of, f"{name}.{col}"
            mn = tables_medium[name][col].min()
            assert mn is None or mn >= start, f"{name}.{col}"
    # planned dates may be in the future
    assert tables_medium["ats_offer_version"]["proposed_start_date"].max() > as_of


def test_future_demand_has_no_future_actual_events(tables_medium, cfg_medium):
    as_of = cfg_medium.dates.as_of
    snap = tables_medium["ats_requisition_snapshot"]
    latest = (
        snap.sort(["requisition_id", "snapshot_date"])
        .group_by("requisition_id", maintain_order=True)
        .agg(pl.all().last())
    )
    future = latest.filter(pl.col("target_hire_date") > as_of)
    assert future.height > 0
    apps = tables_medium["ats_application"].join(future.select("requisition_id"), on="requisition_id")
    assert apps.height > 0, "future requisitions already have applications"
    assert (apps["application_status"] == "active").sum() > 0
    assert apps["status_date"].max() <= as_of


def test_latest_fully_matured_cohort_is_reachable(tables_medium, cfg_medium):
    """Starts exist in March 2026, the latest fully matured cohort for the configured as-of date."""
    hires = tables_medium["hr_worker_event"].filter(pl.col("event_type") == "hire")
    months = set(hires["event_date"].dt.strftime("%Y-%m").unique())
    assert "2026-03" in months and "2026-05" in months
    assert dt.date(2026, 3, 31) + dt.timedelta(days=60) <= cfg_medium.dates.as_of
