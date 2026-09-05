"""Governed populations and the forecast story: quarantine, Time to Fill, FCST-01..04, risk windows.

These are the parts of the story a configuration change could weaken without breaking any
source-level check, so they are asserted here rather than left to inspection.
"""

import datetime as dt

import polars as pl
import pytest

from ta_exec_data_gen.story import (
    SEGMENT_KEYS,
    _latest_snapshot,
    accepted_offers,
    active_pipeline,
    expected_pipeline_fills,
    quarantined_applications,
    stage_yields,
    summarise,
)

BANDS = ["missed", "high_risk", "medium_risk", "on_track"]


def _parts(tables, cfg):
    latest = _latest_snapshot(tables["ats_requisition_snapshot"], cfg.dates.as_of)
    yields = stage_yields(tables, latest, cfg)
    return latest, yields, expected_pipeline_fills(tables, latest, yields)


# ---------------------------------------------------------------- quarantine containment
def test_quarantined_applications_never_reach_a_governed_figure(tables_medium, cfg_medium):
    quarantined = quarantined_applications(tables_medium)
    assert quarantined.height == cfg_medium.offers.quarantine_case_count
    assert (quarantined["accepted_cycles"] > 1).all()

    governed = accepted_offers(tables_medium)
    raw = accepted_offers(tables_medium, include_quarantined=True)
    assert raw.height - governed.height == quarantined.height
    assert governed.join(quarantined, on="application_id", how="semi").height == 0
    assert (governed["cycles"] == 1).all(), "a governed acceptance is one cycle"


def test_quarantined_applications_do_not_train_the_yield_model(tables_medium, cfg_medium):
    """The yield is defined on the governed application fact, which quarantined rows never reach.

    A null-yield check cannot see this: the leaked rows land in the denominator of an
    otherwise healthy segment, so the only way to catch it is to count the training rows.
    """
    latest, yields, _ = _parts(tables_medium, cfg_medium)
    quarantined = quarantined_applications(tables_medium).select("application_id")
    finished = (
        tables_medium["ats_stage_history"]
        .select("application_id", "stage_code")
        .unique()
        .join(tables_medium["ats_application"].select("application_id", "application_status"), on="application_id")
        .filter(pl.col("application_status") != "active")
    )
    with_quarantined = dict(finished.group_by("stage_code").len().iter_rows())
    without = dict(
        finished.join(quarantined, on="application_id", how="anti").group_by("stage_code").len().iter_rows()
    )
    trained = dict(yields.select("stage_code", "n_all").unique().iter_rows())
    for stage, expected in without.items():
        assert trained[stage] == expected, f"{stage}: trained on {trained[stage]}, governed population is {expected}"
        assert with_quarantined[stage] > expected, "the quarantine must actually remove rows at every stage"


def test_time_to_fill_population_follows_the_contract(tables_medium, cfg_medium):
    s = summarise(tables_medium, cfg_medium)
    steps = s["time_to_fill_population"]["applications"].to_list()
    assert sum(steps[:3]) == steps[3], "the reconciliation must add up"
    assert steps[1] == -cfg_medium.offers.quarantine_case_count
    assert steps[2] < 0, "some acceptances sit on requisitions later cancelled"

    latest = _latest_snapshot(tables_medium["ats_requisition_snapshot"], cfg_medium.dates.as_of)
    cancelled = latest.filter(pl.col("requisition_status") == "cancelled").select("requisition_id")
    governed = accepted_offers(tables_medium).join(
        latest.filter(pl.col("requisition_status") != "cancelled").select("requisition_id"),
        on="requisition_id",
        how="semi",
    )
    assert governed.join(cancelled, on="requisition_id", how="semi").height == 0
    # EXEC-05 keeps offers that were later lost: removing them would rewrite the cycle time
    assert governed["lost"].sum() > 0
    assert s["time_to_fill_overall"]["accepted_offers"][0] == governed.height


# ---------------------------------------------------------------- FCST-01 stage yield
def test_stage_yield_respects_the_documented_fallback(tables_medium, cfg_medium):
    latest, yields, _ = _parts(tables_medium, cfg_medium)
    min_obs = cfg_medium.story.forecast_min_segment_observations
    assert yields.height > 0
    assert ((yields["applied_yield"] >= 0) & (yields["applied_yield"] <= 1)).all()
    assert (yields["applied_observations"] >= min_obs).all(), "a segment below the floor must fall back"
    assert set(yields["yield_segment_level"].unique()) <= {"bu_jf_jl", "jf_jl", "jf", "all"}
    # the finest grain that clears the floor is the one used
    used_coarser = yields.filter(pl.col("yield_segment_level") != "bu_jf_jl")
    assert used_coarser.height > 0, "small segments must exercise the fallback"
    assert (used_coarser["n_bu_jf_jl"].fill_null(0) < min_obs).all()
    fine = yields.filter(pl.col("yield_segment_level") == "bu_jf_jl")
    assert (fine["n_bu_jf_jl"] >= min_obs).all()
    # the model must be a per-segment model, not one global number
    interview = yields.filter(pl.col("stage_code") == "interview")
    by_family = interview.group_by("job_family_code").agg(pl.col("applied_yield").mean())
    assert by_family["applied_yield"].max() - by_family["applied_yield"].min() > 0.05


def test_yield_rises_with_stage_depth(tables_medium, cfg_medium):
    _, yields, _ = _parts(tables_medium, cfg_medium)
    by_stage = dict(yields.group_by("stage_code").agg(pl.col("applied_yield").median()).iter_rows())
    assert by_stage["review"] < by_stage["screen"] < by_stage["interview"] < by_stage["offer"]


# ---------------------------------------------------------------- FCST-02 requisition cap
def test_every_active_candidate_gets_a_yield_and_a_gap_is_an_error(tables_medium, cfg_medium):
    """A candidate the model cannot answer for must fail loudly, not quietly forecast zero."""
    latest, yields, _ = _parts(tables_medium, cfg_medium)
    active = active_pipeline(tables_medium, latest)
    assert active.height > 0
    matched = active.join(
        yields.select(*SEGMENT_KEYS, "stage_code", "applied_yield"), on=[*SEGMENT_KEYS, "stage_code"]
    )
    assert matched.height == active.height, "the fallback must cover every segment the pipeline contains"
    assert matched["applied_yield"].null_count() == 0

    # the model is asked about combinations history alone does not contain
    trained_only = stage_yields(tables_medium, latest, cfg_medium)
    assert trained_only.height >= active.select(*SEGMENT_KEYS, "stage_code").unique().height

    needed = active.select(*SEGMENT_KEYS, "stage_code").unique().head(1)
    pruned = yields.join(needed, on=[*SEGMENT_KEYS, "stage_code"], how="anti")
    assert pruned.height == yields.height - 1
    with pytest.raises(ValueError, match="no stage yield"):
        expected_pipeline_fills(tables_medium, latest, pruned)


def test_expected_pipeline_fills_are_capped_at_remaining_openings(tables_medium, cfg_medium):
    latest, _, fills = _parts(tables_medium, cfg_medium)
    open_reqs = latest.filter(pl.col("requisition_status") == "open").select("requisition_id", "openings_position")
    joined = fills.join(open_reqs, on="requisition_id", how="inner")
    assert joined.height == fills.height, "expected fills exist only for open requisitions"
    assert (joined["expected_pipeline_fills"] <= joined["openings_position"] + 1e-9).all()
    assert (joined["expected_pipeline_fills"] >= 0).all()
    binding = joined.filter(pl.col("uncapped_expected_fills") > pl.col("expected_pipeline_fills") + 1e-9)
    assert binding.height > 0, "the cap must actually bind somewhere, otherwise it is untested"


# ---------------------------------------------------------------- FCST-03/04 forecast
def test_forecast_adds_lift_without_exceeding_demand(tables_medium, cfg_medium):
    s = summarise(tables_medium, cfg_medium)
    months = s["forecast_by_thd_month"]
    assert (months["forecast_fill_rate"] >= months["fill_rate"]).all()
    assert (months["forecast_filled"] <= months["requested"] + 1e-9).all()
    assert (months["forecast_fill_rate"] <= 1 + 1e-9).all()

    summary = {r["thd_window"]: r for r in s["forecast_summary"].iter_rows(named=True)}
    overall = summary["all THD"]
    assert overall["forecast_fill_rate"] > overall["fill_rate"], "the pipeline must add visible lift"
    # the lift belongs to demand that has not happened yet, not to closed history
    future = summary["THD after as-of"]
    past = summary["THD on or before as-of"]
    assert future["expected_pipeline_fills"] > past["expected_pipeline_fills"]
    assert future["forecast_fill_rate"] - future["fill_rate"] > past["forecast_fill_rate"] - past["fill_rate"]


def test_forecast_reconciles_between_month_and_summary_grain(tables_medium, cfg_medium):
    s = summarise(tables_medium, cfg_medium)
    months = s["forecast_by_thd_month"]
    overall = s["forecast_summary"].filter(pl.col("thd_window") == "all THD").row(0, named=True)
    assert months["requested"].sum() == overall["requested_positions"]
    assert months["filled"].sum() == overall["filled_positions"]
    assert abs(months["expected_pipeline_fills"].sum() - overall["expected_pipeline_fills"]) < 0.5
    # and the forecast never claims more seats than the pipeline can hold
    latest, _, fills = _parts(tables_medium, cfg_medium)
    open_positions = latest.filter(pl.col("requisition_status") == "open")["openings_position"].sum()
    assert fills["expected_pipeline_fills"].sum() <= open_positions


# ---------------------------------------------------------------- risk under a THD selection
def test_risk_bands_need_a_thd_selection_that_reaches_past_the_as_of_date(tables_medium, cfg_medium):
    """TOAD is on or before THD, so a THD window ending on the as-of date is all Missed.

    This is a property of the metric definition, not a data defect: every band other than
    Missed requires open demand whose Target Hire Date is still in the future. The wireframe
    default (Jan-May 2026 THD) therefore cannot show the four-band mix it draws.
    """
    s = summarise(tables_medium, cfg_medium)
    windows = {r["thd_window"]: r for r in s["risk_band_by_thd_window"].iter_rows(named=True)}
    past = windows["2026-01..2026-05 (wireframe default)"]
    assert past["open_positions"] > 0
    assert past["missed"] == past["open_positions"], "a closed THD window can only be Missed"
    assert past["at_risk_share"] == 1.0

    future = windows["2026-06..2027-05 (future THD)"]
    assert all(future[band] > 0 for band in BANDS), "the four-band mix lives in future THD"
    assert 0 < future["at_risk_share"] < 1

    for window in windows.values():
        assert sum(window[band] for band in BANDS) == window["open_positions"]

    # the underlying source rule the whole thing rests on
    latest = _latest_snapshot(tables_medium["ats_requisition_snapshot"], cfg_medium.dates.as_of)
    assert (latest["target_offer_acceptance_date"] <= latest["target_hire_date"]).all()


# ---------------------------------------------------------------- future demand coverage
def test_planned_demand_reaches_the_future_thd_ceiling(tables_medium, cfg_medium):
    as_of, end = cfg_medium.dates.as_of, cfg_medium.dates.future_thd_end
    horizon = (end - as_of).days
    assert cfg_medium.demand.early_plan_lead_days.max >= horizon, (
        "an annual-plan approval must be able to reach future_thd_end from the as-of date, "
        "otherwise the planned demand fades out before the contract's ceiling"
    )
    latest = _latest_snapshot(tables_medium["ats_requisition_snapshot"], as_of)
    max_thd = latest["target_hire_date"].max()
    assert max_thd <= end
    assert max_thd >= end - dt.timedelta(days=90), f"future demand stops at {max_thd}, well short of {end}"
    # and the near future is continuously covered, not a scatter of isolated months
    future_months = set(
        latest.filter(pl.col("target_hire_date") > as_of)["target_hire_date"].dt.strftime("%Y-%m").unique()
    )
    expected = {f"2026-{m:02d}" for m in range(6, 13)}
    assert expected <= future_months, sorted(expected - future_months)
