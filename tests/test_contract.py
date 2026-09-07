"""Alignment with the ta-exec-db contract: vocabulary, columns, boundaries, no derived fields.

The column lists below are the contract's minimum columns for each logical file (with the
generator's documented source-system names) plus the source-realism columns the data
dictionary maps. If contract 1.3 gains or renames a field, this test is what fails first.
"""

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
    "started",
    "offer_rescinded",
    "candidate_renege",
}
OFFER_STATUSES = {"pending", "accepted", "offer_declined", "offer_withdrawn", "offer_rescinded", "candidate_renege"}
STAGE_EXIT_REASONS = {"rejected", "withdrawn", "offer_declined", "offer_withdrawn"}
TIMESTAMPS = ["updated_at", "extracted_at"]

# contract 1.3, "Required source files": the minimum columns every logical file must carry
CONTRACT_MINIMUM_COLUMNS = {
    "ats_requisition_snapshot": [
        "requisition_id",
        "snapshot_date",
        "requisition_status",
        "approval_date",
        "target_hire_date",
        "target_offer_acceptance_date",
        "requested_positions",
        "openings_position",
        "cancelled_positions",
        "business_unit_code",
        "job_family_code",
        "job_level_code",
        "hiring_constraint_code",
        "updated_at",
        "extracted_at",
    ],
    "ats_application": [
        "application_id",
        "candidate_id",
        "requisition_id",
        "application_date",
        "application_status_current",
        "current_stage_code",
        "rejected_date",
        "withdrawal_date",
        "disposition_reason",
        "updated_at",
        "extracted_at",
    ],
    "ats_offer": [
        "application_id",
        "requisition_id",
        "offer_status_current",
        "offer_extended_date",
        "offer_accepted_date",
        "offer_declined_date",
        "offer_withdrawn_date",
        "offer_rescinded_date",
        "candidate_renege_date",
        "planned_start_date",
        "updated_at",
        "extracted_at",
    ],
    "ats_stage_history": [
        "stage_event_id",
        "application_id",
        "stage_sequence_number",
        "stage_code",
        "stage_entry_date",
        "stage_exit_date",
        "exit_reason",
        "updated_at",
        "extracted_at",
    ],
    "hr_worker_event": [
        "worker_event_id",
        "worker_id",
        "application_id",
        "event_type",
        "event_date",
        "termination_reason",
        "updated_at",
        "extracted_at",
    ],
    "ats_business_unit": ["business_unit_code", "business_unit_name", "updated_at", "extracted_at"],
    "ats_job_family": ["job_family_code", "job_family_name", "updated_at", "extracted_at"],
    "ats_job_level": ["job_level_code", "job_level_name", "updated_at", "extracted_at"],
}

EXPECTED_COLUMNS = {
    "ats_business_unit": ["business_unit_code", "business_unit_name", "sort_order", "is_active", *TIMESTAMPS],
    "ats_job_family": ["job_family_code", "job_family_name", "sort_order", "is_active", *TIMESTAMPS],
    "ats_job_level": ["job_level_code", "job_level_name", "level_rank", "is_active", *TIMESTAMPS],
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
        "hiring_constraint_code",
        *TIMESTAMPS,
    ],
    "ats_application": [
        "application_id",
        "candidate_id",
        "requisition_id",
        "application_date",
        "source_channel",
        "application_status_current",
        "current_stage_code",
        "rejected_date",
        "withdrawal_date",
        "disposition_reason",
        *TIMESTAMPS,
    ],
    "ats_stage_history": [
        "stage_event_id",
        "application_id",
        "stage_code",
        "stage_sequence_number",
        "stage_entry_date",
        "stage_exit_date",
        "exit_reason",
        *TIMESTAMPS,
    ],
    "ats_offer": [
        "application_id",
        "requisition_id",
        "offer_status_current",
        "offer_extended_date",
        "offer_accepted_date",
        "offer_declined_date",
        "offer_withdrawn_date",
        "offer_rescinded_date",
        "candidate_renege_date",
        "planned_start_date",
        "base_salary",
        "currency",
        *TIMESTAMPS,
    ],
    "hr_worker_event": [
        "worker_event_id",
        "worker_id",
        "candidate_id",
        "application_id",
        "requisition_id",
        "event_type",
        "event_date",
        "termination_reason",
        *TIMESTAMPS,
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


def test_every_contract_minimum_column_is_present(tables_small):
    for name, required in CONTRACT_MINIMUM_COLUMNS.items():
        missing = set(required) - set(tables_small[name].columns)
        assert not missing, f"{name} is missing contract columns {sorted(missing)}"


def test_no_derived_analytics_columns(tables_small):
    for name, frame in tables_small.items():
        for col in frame.columns:
            assert not any(marker in col for marker in DERIVED_MARKERS), f"{name}.{col}"


def test_no_offer_version_or_cycle_identifiers(tables_small):
    """Contract 1.3: one current offer row per application, and no version/cycle model."""
    off = tables_small["ats_offer"]
    for banned in ("offer_cycle", "offer_version", "is_current_version", "version_reason", "offer_id"):
        assert not any(banned in col for col in off.columns), banned
    assert off.group_by("application_id").len()["len"].max() == 1


def test_vocabularies_match_contract(tables_small, cfg_small):
    stg = tables_small["ats_stage_history"]
    assert set(stg["stage_code"].unique()) <= set(STAGE_ORDER)
    seq = stg.select("stage_code", "stage_sequence_number").unique()
    assert all(STAGE_ORDER[s - 1] == c for c, s in seq.iter_rows())
    assert set(stg["exit_reason"].drop_nulls().unique()) <= STAGE_EXIT_REASONS
    snap = tables_small["ats_requisition_snapshot"]
    assert set(snap["hiring_constraint_code"].unique()) <= set(CONSTRAINTS)
    assert set(snap["requisition_status"].unique()) == {"open", "filled", "cancelled"}
    assert set(tables_small["ats_application"]["application_status_current"].unique()) == APPLICATION_STATUSES
    assert set(tables_small["ats_offer"]["offer_status_current"].unique()) <= OFFER_STATUSES
    assert set(tables_small["hr_worker_event"]["event_type"].unique()) == {"start", "termination"}
    assert cfg_small.hiring_constraints == CONSTRAINTS


def test_raw_timestamps_follow_the_contract(tables_small, cfg_small):
    extracted_at = cfg_small.timestamps.extracted_at
    cutoff = dt.datetime.combine(cfg_small.dates.as_of, dt.time(23, 59, 59))
    for name, frame in tables_small.items():
        assert frame["extracted_at"].dtype == pl.Datetime("us"), name
        assert frame["updated_at"].null_count() == 0, name
        assert (frame["extracted_at"] == extracted_at).all(), name
        assert (frame["updated_at"] <= frame["extracted_at"]).all(), name
        assert frame["updated_at"].max() <= cutoff, name
    # a repeated unchanged row keeps the update timestamp it already had
    snap = tables_small["ats_requisition_snapshot"].sort(["requisition_id", "snapshot_date"])
    repeated = snap.group_by("requisition_id", "updated_at").len().filter(pl.col("len") > 1)
    assert repeated.height > 0, "monthly extracts must repeat unchanged requisition rows"
    moved = snap.group_by("requisition_id").agg(pl.col("updated_at").n_unique().alias("n")).filter(pl.col("n") > 1)
    assert moved.height > 0, "a real source change must advance updated_at"


def test_every_requisition_is_in_the_as_of_extract(tables_medium, cfg_medium):
    snap = tables_medium["ats_requisition_snapshot"]
    as_of_rows = snap.filter(pl.col("snapshot_date") == cfg_medium.dates.as_of)
    assert as_of_rows.height == snap["requisition_id"].n_unique()
    assert as_of_rows["requisition_id"].n_unique() == as_of_rows.height


def test_reporting_boundaries(tables_medium, cfg_medium):
    as_of, start, end = cfg_medium.dates.as_of, cfg_medium.dates.history_start, cfg_medium.dates.future_thd_end
    snap = tables_medium["ats_requisition_snapshot"]
    assert snap["target_hire_date"].max() <= end
    assert snap["target_hire_date"].max() > as_of, "future demand must exist"
    assert snap["snapshot_date"].max() == as_of
    assert snap["approval_date"].min() >= start
    for name, cols in {
        "ats_application": ["application_date", "rejected_date", "withdrawal_date"],
        "ats_stage_history": ["stage_entry_date", "stage_exit_date"],
        "ats_offer": [
            "offer_extended_date",
            "offer_accepted_date",
            "offer_declined_date",
            "offer_withdrawn_date",
            "offer_rescinded_date",
            "candidate_renege_date",
        ],
        "hr_worker_event": ["event_date"],
    }.items():
        for col in cols:
            mx = tables_medium[name][col].max()
            assert mx is None or mx <= as_of, f"{name}.{col}"
            mn = tables_medium[name][col].min()
            assert mn is None or mn >= start, f"{name}.{col}"
    # planned dates may be in the future
    assert tables_medium["ats_offer"]["planned_start_date"].max() > as_of


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
    assert (apps["application_status_current"] == "active").sum() > 0
    assert apps["rejected_date"].max() is None or apps["rejected_date"].max() <= as_of


def test_latest_fully_matured_cohort_is_reachable(tables_medium, cfg_medium):
    """Starts exist in March 2026, the latest fully matured cohort for the configured as-of date."""
    starts = tables_medium["hr_worker_event"].filter(pl.col("event_type") == "start")
    months = set(starts["event_date"].dt.strftime("%Y-%m").unique())
    assert "2026-03" in months and "2026-05" in months
    assert dt.date(2026, 3, 31) + dt.timedelta(days=60) <= cfg_medium.dates.as_of
