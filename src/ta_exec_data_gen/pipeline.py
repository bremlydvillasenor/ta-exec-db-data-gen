"""End-to-end assembly: configuration -> simulation -> raw source frames.

`generate(cfg)` returns the eight raw source tables as Polars DataFrames with real dates,
business identifiers and the two raw timestamps every file carries (`updated_at`,
`extracted_at`). Nothing analytics-ready is produced: no flags, no bands, no cohort
maturity, no yields.

The file names keep their source-system prefix (`ats_`, `hr_`); `docs/data_dictionary.md`
holds the one-to-one mapping onto the logical file names in the contract, which is the
option the contract allows an existing generator.
"""

from __future__ import annotations

import logging

import numpy as np
import polars as pl

from .config import GeneratorConfig
from .dates import DayIndex
from .funnel import NO_DAY, STAGES, FunnelSimulator, outcomes_to_frames
from .hr import build_worker_events
from .offers import build_offers
from .reference import build_business_units, build_job_families, build_job_levels
from .requisitions import build_requisition_master
from .rng import RngFactory
from .snapshots import build_snapshots
from .timestamps import Timestamps

log = logging.getLogger("ta_exec_data_gen")

OUTPUT_TABLES = [
    "ats_business_unit",
    "ats_job_family",
    "ats_job_level",
    "ats_requisition_snapshot",
    "ats_application",
    "ats_stage_history",
    "ats_offer",
    "hr_worker_event",
]

SOURCE_CHANNELS = ["career_site", "job_board", "linkedin", "referral", "agency", "internal", "sourced"]
SOURCE_CHANNEL_WEIGHTS = [0.30, 0.25, 0.18, 0.10, 0.05, 0.04, 0.08]


def _day_to_date(idx: DayIndex, *columns: str) -> list[pl.Expr]:
    """Integer day-offset columns to Date, NO_DAY becoming null."""
    return [
        pl.when(pl.col(c) == NO_DAY).then(pl.lit(None, dtype=pl.Date)).otherwise(idx.expr(c)).alias(c) for c in columns
    ]


def _split_conflicting_reuse(
    candidate: np.ndarray,
    holds_seat: np.ndarray,
    in_pipeline: np.ndarray,
    applied_on: np.ndarray,
    seat_taken_on: np.ndarray,
) -> np.ndarray:
    """Undo any candidate merge that would make one person hold two jobs at once.

    Reusing a candidate across requisitions is realistic, but a real person cannot hold two
    live acceptances at the same time, cannot still be an active candidate somewhere while
    already holding one, and does not start applying again after taking a seat. Applications
    are visited in application-date order; the one that would create the conflict is handed
    back its own candidate, so the reuse rate drops slightly instead of the source data
    describing an impossible person.

    Visiting in application-date order is what makes one pass enough: an acceptance never
    precedes its own application, so a merge accepted now can only ever be invalidated by an
    earlier application, and those have already been seen.
    """
    resolved = candidate.copy()
    pipeline: dict[int, bool] = {}
    seat_day: dict[int, int] = {}
    for i in range(resolved.size):
        key = int(resolved[i])
        if key != i:
            took_seat = seat_day.get(key)
            conflict = (
                (holds_seat[i] and (took_seat is not None or pipeline.get(key, False)))
                or (in_pipeline[i] and took_seat is not None)
                or (took_seat is not None and applied_on[i] > took_seat)
            )
            if conflict:
                resolved[i] = i
                key = i
        if holds_seat[i]:
            taken = int(seat_taken_on[i])
            seat_day[key] = min(seat_day.get(key, taken), taken)
        if in_pipeline[i]:
            pipeline[key] = True
    return resolved


def assign_candidates(apps: pl.DataFrame, cfg: GeneratorConfig, rngs: RngFactory) -> pl.DataFrame:
    """Give every application a candidate; some candidates apply to several requisitions."""
    rng = rngs.stream("candidates")
    n = apps.height
    candidate = np.arange(n)
    reuse = rng.random(n) < cfg.funnel.candidate_pool_reuse
    donor = rng.integers(0, n, size=n)
    req = apps["req_idx"].to_numpy()
    # only reuse a candidate who applied to a different requisition, and only once per pair
    ok = reuse & (req[donor] != req) & (donor < np.arange(n))
    candidate = np.where(ok, candidate[donor], candidate)
    apps = apps.with_columns(candidate_raw=pl.Series(candidate))
    apps = (
        apps.with_columns(dup=pl.col("app_idx").rank("ordinal").over("candidate_raw", "req_idx") > 1)
        .with_columns(candidate_raw=pl.when(pl.col("dup")).then(pl.col("app_idx")).otherwise(pl.col("candidate_raw")))
        .drop("dup")
    )
    # a live acceptance takes the seat; an active application keeps the person in a pipeline
    holds_seat = (apps["offer_accepted_day"].to_numpy() != NO_DAY) & (
        (apps["offer_rescinded_day"].to_numpy() == NO_DAY) & (apps["candidate_renege_day"].to_numpy() == NO_DAY)
    )
    in_pipeline = (apps["status"] == "active").to_numpy()
    applied_on = apps["application_day"].to_numpy()
    seat_taken_on = apps["offer_accepted_day"].to_numpy()
    row_of_app = np.argsort(apps["app_idx"].to_numpy(), kind="stable")
    fixed = _split_conflicting_reuse(
        apps["candidate_raw"].to_numpy()[row_of_app],
        holds_seat[row_of_app],
        in_pipeline[row_of_app],
        applied_on[row_of_app],
        seat_taken_on[row_of_app],
    )
    resolved = np.empty_like(fixed)
    resolved[row_of_app] = fixed
    apps = apps.with_columns(candidate_raw=pl.Series(resolved))
    # renumber candidates by first application so ids look natural
    first_seen = (
        apps.group_by("candidate_raw")
        .agg(first_app=pl.col("app_idx").min())
        .sort("first_app")
        .with_row_index("candidate_seq")
    )
    apps = apps.join(first_seen.select("candidate_raw", "candidate_seq"), on="candidate_raw", how="left")
    channel = rng.choice(np.asarray(SOURCE_CHANNELS, dtype=object), size=n, p=SOURCE_CHANNEL_WEIGHTS)
    return apps.with_columns(
        candidate_id=pl.format("CAND-{}", (pl.col("candidate_seq") + 1).cast(pl.Utf8).str.zfill(7)),
        source_channel=pl.Series(channel.astype(str)),
    ).drop("candidate_raw", "candidate_seq")


def generate(cfg: GeneratorConfig) -> dict[str, pl.DataFrame]:
    rngs = RngFactory(cfg.seed)
    idx = DayIndex(cfg.dates.history_start)
    stamps = Timestamps(cfg)

    log.info("building requisition master")
    master = build_requisition_master(cfg, rngs)
    log.info("requisitions: %d", master.height)

    log.info("simulating candidate funnels")
    outcomes = FunnelSimulator(cfg, rngs).simulate_all(master)
    req_state, apps, stage = outcomes_to_frames(outcomes)
    log.info("applications: %d, stage entries: %d", apps.height, stage.height)

    apps = assign_candidates(apps, cfg, rngs)
    offers = build_offers(apps, master, cfg, rngs)
    log.info("current offers: %d", offers.height)
    hr_events = build_worker_events(apps, master, cfg, rngs)
    log.info("hr worker events: %d", hr_events.height)
    snapshots = build_snapshots(master, req_state, apps, stage, cfg)
    log.info("requisition snapshots: %d", snapshots.height)

    # ------------------------------------------------------------------ ids
    apps = apps.with_columns(
        application_id=pl.format("APP-{}", (pl.col("app_idx") + 1).cast(pl.Utf8).str.zfill(7)),
    ).join(master.select("req_idx", "requisition_id"), on="req_idx", how="left")
    app_ids = apps.select("app_idx", "application_id", "requisition_id", "candidate_id")

    # ------------------------------------------------------------------ ats_application
    current_stage = (
        stage.sort(["app_idx", "stage_sequence"])
        .group_by("app_idx", maintain_order=True)
        .agg(current_stage_code=pl.col("stage_code").last())
    )
    last_stage_event = stage.group_by("app_idx").agg(
        last_stage_day=pl.max_horizontal(pl.col("stage_entry_day").max(), pl.col("stage_exit_day").max())
    )
    ats_application = (
        apps.join(current_stage, on="app_idx", how="left")
        .join(last_stage_event, on="app_idx", how="left")
        .with_columns(
            # the application record last moved on the latest of: it was submitted, its
            # stage moved, or its status changed (disposition, acceptance, loss, start)
            app_changed_day=pl.max_horizontal("application_day", "status_day", "last_stage_day"),
            rejected_day=pl.when(pl.col("status") == "rejected").then(pl.col("status_day")).otherwise(NO_DAY),
            withdrawal_day=pl.when(pl.col("status") == "withdrawn").then(pl.col("status_day")).otherwise(NO_DAY),
        )
        .with_columns(*_day_to_date(idx, "application_day", "rejected_day", "withdrawal_day"))
        .sort("application_id")
        .select(
            "application_id",
            "candidate_id",
            "requisition_id",
            application_date=pl.col("application_day"),
            source_channel=pl.col("source_channel"),
            application_status_current=pl.col("status"),
            current_stage_code=pl.col("current_stage_code"),
            rejected_date=pl.col("rejected_day"),
            withdrawal_date=pl.col("withdrawal_day"),
            disposition_reason=pl.col("disposition_reason"),
            app_changed_day=pl.col("app_changed_day"),
        )
    )
    ats_application = stamps.stamp(ats_application, "app_changed_day", "application_id")

    # ------------------------------------------------------------------ ats_stage_history
    ats_stage_history = (
        stage.join(app_ids.select("app_idx", "application_id"), on="app_idx", how="left")
        .sort(["app_idx", "stage_sequence"])
        .with_row_index("seq")
        .with_columns(
            # a stage row is written on entry and updated when its exit is recorded
            stage_changed_day=pl.when(pl.col("stage_exit_day") == NO_DAY)
            .then(pl.col("stage_entry_day"))
            .otherwise(pl.col("stage_exit_day")),
        )
        .with_columns(*_day_to_date(idx, "stage_entry_day", "stage_exit_day"))
        .select(
            stage_event_id=pl.format("STG-{}", (pl.col("seq") + 1).cast(pl.Utf8).str.zfill(8)),
            application_id=pl.col("application_id"),
            stage_code=pl.col("stage_code"),
            stage_sequence_number=pl.col("stage_sequence"),
            stage_entry_date=pl.col("stage_entry_day"),
            stage_exit_date=pl.col("stage_exit_day"),
            exit_reason=pl.col("exit_reason"),
            stage_changed_day=pl.col("stage_changed_day"),
        )
    )
    ats_stage_history = stamps.stamp(ats_stage_history, "stage_changed_day", "stage_event_id")

    # ------------------------------------------------------------------ ats_offer
    ats_offer = (
        offers.join(app_ids, on="app_idx", how="left")
        .with_columns(
            *_day_to_date(
                idx,
                "offer_extended_day",
                "offer_accepted_day",
                "offer_declined_day",
                "offer_withdrawn_day",
                "offer_rescinded_day",
                "candidate_renege_day",
                "planned_start_day",
            )
        )
        .sort("application_id")
        .select(
            application_id=pl.col("application_id"),
            requisition_id=pl.col("requisition_id"),
            offer_status_current=pl.col("offer_status_current"),
            offer_extended_date=pl.col("offer_extended_day"),
            offer_accepted_date=pl.col("offer_accepted_day"),
            offer_declined_date=pl.col("offer_declined_day"),
            offer_withdrawn_date=pl.col("offer_withdrawn_day"),
            offer_rescinded_date=pl.col("offer_rescinded_day"),
            candidate_renege_date=pl.col("candidate_renege_day"),
            planned_start_date=pl.col("planned_start_day"),
            base_salary=pl.col("base_salary"),
            currency=pl.col("currency"),
            offer_changed_day=pl.col("offer_changed_day"),
        )
    )
    ats_offer = stamps.stamp(ats_offer, "offer_changed_day", "application_id")

    # ------------------------------------------------------------------ hr_worker_event
    hr_worker_event = (
        hr_events.join(app_ids, on="app_idx", how="left")
        .with_columns(*_day_to_date(idx, "event_day"))
        .select(
            worker_event_id=pl.format("WE-{}", (pl.col("event_seq") + 1).cast(pl.Utf8).str.zfill(7)),
            worker_id=pl.col("worker_id"),
            candidate_id=pl.col("candidate_id"),
            application_id=pl.col("application_id"),
            requisition_id=pl.col("requisition_id"),
            event_type=pl.col("event_type"),
            event_date=pl.col("event_day"),
            termination_reason=pl.col("termination_reason"),
            event_changed_day=pl.col("event_changed_day"),
        )
    )
    hr_worker_event = stamps.stamp(hr_worker_event, "event_changed_day", "worker_event_id")

    # ------------------------------------------------------------------ ats_requisition_snapshot
    ats_requisition_snapshot = (
        snapshots.with_columns(
            *_day_to_date(idx, "snapshot_day", "approval_day", "thd_day_snapshot", "toad_day_snapshot")
        )
        .select(
            snapshot_date=pl.col("snapshot_day"),
            requisition_id=pl.col("requisition_id"),
            requisition_title=pl.col("requisition_title"),
            business_unit_code=pl.col("bu_code"),
            job_family_code=pl.col("jf_code"),
            job_level_code=pl.col("level_code"),
            work_location=pl.col("work_location"),
            hiring_manager_id=pl.col("hiring_manager_id"),
            recruiter_id=pl.col("recruiter_id"),
            requisition_status=pl.col("requisition_status"),
            approval_date=pl.col("approval_day"),
            target_hire_date=pl.col("thd_day_snapshot"),
            target_offer_acceptance_date=pl.col("toad_day_snapshot"),
            requested_positions=pl.col("requested_positions"),
            openings_position=pl.col("openings_position"),
            cancelled_positions=pl.col("cancelled_positions"),
            hiring_constraint_code=pl.col("hiring_constraint_code"),
            # one requisition record, one change moment: monthly extracts that repeat an
            # unchanged row must repeat its updated_at, never restamp it
            change_key=pl.format("{}|{}", pl.col("requisition_id"), pl.col("last_change_day")),
            last_change_day=pl.col("last_change_day"),
        )
    )
    ats_requisition_snapshot = stamps.stamp(ats_requisition_snapshot, "last_change_day", "change_key").drop(
        "change_key"
    )

    tables = {
        "ats_business_unit": stamps.stamp_reference(build_business_units(cfg)),
        "ats_job_family": stamps.stamp_reference(build_job_families(cfg)),
        "ats_job_level": stamps.stamp_reference(build_job_levels(cfg)),
        "ats_requisition_snapshot": ats_requisition_snapshot,
        "ats_application": ats_application,
        "ats_stage_history": ats_stage_history,
        "ats_offer": ats_offer,
        "hr_worker_event": hr_worker_event,
    }
    assert list(tables) == OUTPUT_TABLES
    assert set(STAGES) == set(cfg.funnel.stages)
    return tables
