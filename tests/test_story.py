"""The intended data story must emerge from the records (checked on a medium-size run)."""

import polars as pl

from ta_exec_data_gen.story import accepted_offers, summarise


def _summary(tables_medium, cfg_medium):
    return summarise(tables_medium, cfg_medium)


def test_segments_differ_in_fill_and_speed(tables_medium, cfg_medium):
    s = _summary(tables_medium, cfg_medium)
    fill = {r["job_family_code"]: r["fill_rate"] for r in s["fill_by_job_family"].iter_rows(named=True)}
    ttf = {r["job_family_code"]: r["median_ttf_days"] for r in s["time_to_fill_by_job_family"].iter_rows(named=True)}
    assert fill["DAT"] < fill["OPS"]
    assert fill["SWE"] < fill["SLS"]
    assert ttf["DAT"] > ttf["SLS"] and ttf["SWE"] > ttf["OPS"]


def test_risk_bands_and_constraints_are_populated(tables_medium, cfg_medium):
    s = _summary(tables_medium, cfg_medium)
    bands = {r["risk_band"]: r["open_positions"] for r in s["open_positions_by_risk_band"].iter_rows(named=True)}
    assert bands.get("missed", 0) > 0 and bands.get("on_track", 0) > 0
    assert (bands.get("high_risk", 0) + bands.get("medium_risk", 0)) > 0
    by_risk = s["constraint_by_risk"]
    share = lambda band: (  # noqa: E731
        by_risk.filter(
            (pl.col("risk_band") == band) & (pl.col("primary_hiring_constraint") == "no_material_constraint")
        )["open_positions"].sum()
        / max(by_risk.filter(pl.col("risk_band") == band)["open_positions"].sum(), 1)
    )
    assert share("on_track") > share("missed"), "missed seats should carry material constraints"
    total_open = s["requisition_status_as_of"].filter(pl.col("requisition_status") == "open")["open_positions"][0]
    assert s["open_positions_by_constraint"]["open_positions"].sum() == total_open
    assert sum(bands.values()) == total_open


def test_funnel_bottleneck_and_active_pipeline(tables_medium, cfg_medium):
    s = _summary(tables_medium, cfg_medium)
    funnel = {r["stage_code"]: r for r in s["funnel_by_stage"].iter_rows(named=True)}
    assert funnel["interview"]["conversion"] < funnel["screen"]["conversion"]
    assert funnel["interview"]["median_days"] >= funnel["screen"]["median_days"]
    assert all(funnel[stage]["active_now"] > 0 for stage in ["review", "screen", "assessment", "interview", "offer"])
    ic = {r["job_family_code"]: r for r in s["interview_conversion_by_job_family"].iter_rows(named=True)}
    assert ic["DAT"]["median_days"] > ic["OPS"]["median_days"]


def test_post_acceptance_outcomes_and_source_quirks(tables_medium, cfg_medium):
    app = tables_medium["ats_application"]
    statuses = set(app["application_status"].unique())
    assert {
        "candidate_renege",
        "offer_rescinded",
        "offer_declined",
        "offer_withdrawn",
        "withdrawn",
        "rejected",
        "offer_accepted",
        "active",
    } <= statuses
    ov = tables_medium["ats_offer_version"]
    acc = accepted_offers(tables_medium)
    multi = (
        ov.filter(pl.col("offer_accepted_date").is_not_null())
        .group_by("application_id")
        .agg(versions=pl.len(), cycles=pl.col("offer_id").n_unique())
    )
    assert multi.filter((pl.col("versions") > 1) & (pl.col("cycles") == 1)).height > 0, "administrative revisions"
    assert multi.filter(pl.col("cycles") > 1).height == cfg_medium.offers.quarantine_case_count
    # the ambiguous cases are held out of the governed population, not collapsed into it
    assert acc.filter(pl.col("cycles") > 1).height == 0
    assert acc.height + cfg_medium.offers.quarantine_case_count == multi.height
    assert set(ov["version_reason"].unique()) >= {"initial", "negotiation_revision", "start_date_revision"}
    assert (ov["offer_status"] == "superseded").sum() > 0
    hr = tables_medium["hr_worker_event"]
    hires = hr.filter(pl.col("event_type") == "hire")
    assert hires.group_by("application_id").len().filter(pl.col("len") > 1).height > 0, "duplicate hire rows"
    terms = hr.filter(pl.col("event_type") == "termination")
    assert terms.group_by("worker_id").len().filter(pl.col("len") > 1).height > 0, "duplicate termination rows"
    pending = app.filter(pl.col("application_status") == "offer_accepted").join(
        hires.select("application_id").unique(), on="application_id", how="anti"
    )
    assert pending.height > 0, "accepted offers waiting for a start date"


def test_requisitions_reopen_after_post_acceptance_loss(tables_medium):
    snap = tables_medium["ats_requisition_snapshot"].sort(["requisition_id", "snapshot_date"])
    prev = snap.with_columns(prev_status=pl.col("requisition_status").shift(1).over("requisition_id"))
    reopened = prev.filter((pl.col("prev_status") == "filled") & (pl.col("requisition_status") == "open"))
    assert reopened.height > 0
    multi = snap.group_by("requisition_id").agg(pl.col("requested_positions").max().alias("mx"))
    assert (multi["mx"] > 1).sum() > 0, "multi-position requisitions"
    assert (snap["cancelled_positions"] > 0).sum() > 0
    rebased = (
        snap.group_by("requisition_id").agg(pl.col("target_hire_date").n_unique().alias("n")).filter(pl.col("n") > 1)
    )
    assert rebased.height > 0, "re-baselined target dates"


def test_surge_hires_leave_earlier_but_not_deterministically(tables_medium, cfg_medium):
    s = _summary(tables_medium, cfg_medium)
    cohorts = s["early_attrition_by_cohort"].filter(pl.col("cohort") >= "2025-04")
    surge = cohorts.filter(pl.col("cohort").is_in(["2025-07", "2025-08", "2025-09", "2025-10"]))
    rest = cohorts.filter(~pl.col("cohort").is_in(["2025-07", "2025-08", "2025-09", "2025-10"]))
    surge_rate = surge["early_exits"].sum() / surge["hires"].sum()
    rest_rate = rest["early_exits"].sum() / rest["hires"].sum()
    assert surge_rate > rest_rate
    assert surge["median_ttf_days"].mean() < rest["median_ttf_days"].mean()
    # no perfect relationship: at least one non-surge cohort is worse than the surge average
    assert (rest["early_attrition_rate"] > surge_rate).any()
