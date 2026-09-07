"""Requisition snapshot history: the monthly ATS requisition extract.

Every month-end from the approval month to the as-of date, each requisition appears with
its state on that day: status, requested seats, open seats, cancelled seats, current
Target Hire Date and Target Offer Acceptance Date (both can be re-baselined while the
requisition is open past its TOAD) and the primary hiring constraint the recruiter
recorded. Retained history stops a configurable number of days after a requisition
closes, but **the as-of extract always covers every requisition**, as the contract
requires: a long-closed requisition reappears there unchanged, with the `updated_at` it
had when it last really changed.

`last_change_day` is the day the requisition record itself last moved — approved,
re-baselined, cancelled, or a seat filled or reopened, or the recorded constraint
changed. It is always on or before the snapshot date, so `updated_at` can never claim a
change the snapshot could not yet know about.

`openings_position` is a source quantity here, as the contract requires. It is computed
from the same acceptance and loss events the application and offer files carry, so the
identity requested = active fills + openings holds on every snapshot by construction.

The primary hiring constraint is chosen from evidence on the snapshot day (pipeline
thinness, candidates stuck late in the process, recent declines and reneges, days past
TOAD) plus a per-requisition random tendency, so constraints line up with risk and funnel
behaviour instead of being drawn independently.
"""

from __future__ import annotations

import polars as pl

from .config import GeneratorConfig
from .dates import DayIndex, month_ends
from .funnel import NO_DAY

LATE_STAGES = ["interview", "offer"]
MATERIAL = [
    "qualified_candidates",
    "hiring_manager_delay",
    "compensation",
    "interview_capacity",
    "niche_skills",
    "candidate_availability",
]


def _jf_constraint_table(cfg: GeneratorConfig) -> pl.DataFrame:
    rows = []
    for jf in cfg.job_families:
        weights = [jf.constraint_weights.get(code, 0.0) for code in MATERIAL]
        total = sum(weights) or 1.0
        lo = 0.0
        for code, w in zip(MATERIAL, weights, strict=True):
            hi = lo + w / total
            rows.append({"jf_code": jf.code, "generic_constraint": code, "lo": lo, "hi": hi})
            lo = hi
        rows.append({"jf_code": jf.code, "generic_constraint": MATERIAL[-1], "lo": lo, "hi": 1.01})
    return pl.DataFrame(rows)


def build_snapshots(
    master: pl.DataFrame, req_state: pl.DataFrame, apps: pl.DataFrame, stage: pl.DataFrame, cfg: GeneratorConfig
) -> pl.DataFrame:
    idx = DayIndex(cfg.dates.history_start)
    thd_end = idx.to_day(cfg.dates.future_thd_end)
    keep_days = cfg.requisitions.snapshot_keep_days_after_close
    snap_days = pl.DataFrame(
        {"snapshot_day": [idx.to_day(d) for d in month_ends(cfg.dates.history_start, cfg.dates.as_of)]}
    )

    req = master.join(req_state, on="req_idx", how="left", suffix="_sim")
    grid = (
        req.select("req_idx", "approval_day")
        .join(snap_days, how="cross")
        .filter(pl.col("snapshot_day") >= pl.col("approval_day"))
        .select("req_idx", "snapshot_day")
    )
    as_of_day = idx.to_day(cfg.dates.as_of)

    # ------------------------------------------------------------ fills per snapshot
    accepted = apps.filter(pl.col("offer_accepted_day") != NO_DAY).select(
        "req_idx",
        "offer_accepted_day",
        loss_day=pl.max_horizontal("offer_rescinded_day", "candidate_renege_day"),
    )
    fills = (
        grid.join(accepted, on="req_idx", how="inner")
        .filter(
            (pl.col("offer_accepted_day") <= pl.col("snapshot_day"))
            & ((pl.col("loss_day") == NO_DAY) | (pl.col("loss_day") > pl.col("snapshot_day")))
        )
        .group_by("req_idx", "snapshot_day")
        .agg(fills=pl.len(), last_fill_day=pl.col("offer_accepted_day").max())
    )
    # every seat movement known by the snapshot date: an acceptance filled a seat, a
    # post-acceptance loss reopened one. Both change the requisition record.
    seat_events = (
        grid.join(accepted, on="req_idx", how="inner")
        .with_columns(
            accepted_known=pl.when(pl.col("offer_accepted_day") <= pl.col("snapshot_day"))
            .then(pl.col("offer_accepted_day"))
            .otherwise(pl.lit(NO_DAY)),
            loss_known=pl.when(
                (pl.col("loss_day") != NO_DAY) & (pl.col("loss_day") <= pl.col("snapshot_day"))
            )
            .then(pl.col("loss_day"))
            .otherwise(pl.lit(NO_DAY)),
        )
        .group_by("req_idx", "snapshot_day")
        .agg(last_seat_event_day=pl.max_horizontal("accepted_known", "loss_known").max())
    )

    # ------------------------------------------------------------ pipeline evidence
    app_state = apps.select(
        "req_idx",
        "app_idx",
        "application_day",
        "status",
        exit_day=pl.when(pl.col("status") == "active").then(pl.lit(NO_DAY)).otherwise(pl.col("status_day")),
        declined_day=pl.col("offer_declined_day"),
        renege_day=pl.col("candidate_renege_day"),
    )
    evidence = (
        grid.join(app_state, on="req_idx", how="inner")
        .group_by("req_idx", "snapshot_day")
        .agg(
            active_total=(
                (pl.col("application_day") <= pl.col("snapshot_day"))
                & ((pl.col("exit_day") == NO_DAY) | (pl.col("exit_day") > pl.col("snapshot_day")))
            ).sum(),
            declines_recent=(
                (pl.col("declined_day") != NO_DAY)
                & (pl.col("declined_day") <= pl.col("snapshot_day"))
                & (pl.col("declined_day") > pl.col("snapshot_day") - 90)
            ).sum(),
            reneges_recent=(
                (pl.col("renege_day") != NO_DAY)
                & (pl.col("renege_day") <= pl.col("snapshot_day"))
                & (pl.col("renege_day") > pl.col("snapshot_day") - 120)
            ).sum(),
        )
    )
    late = (
        stage.filter(pl.col("stage_code").is_in(LATE_STAGES))
        .join(apps.select("app_idx", "req_idx"), on="app_idx", how="left")
        .select("req_idx", "stage_entry_day", "stage_exit_day")
    )
    late_active = (
        grid.join(late, on="req_idx", how="inner")
        .filter(
            (pl.col("stage_entry_day") <= pl.col("snapshot_day"))
            & ((pl.col("stage_exit_day") == NO_DAY) | (pl.col("stage_exit_day") > pl.col("snapshot_day")))
        )
        .group_by("req_idx", "snapshot_day")
        .agg(late_active=pl.len())
    )

    jf_table = _jf_constraint_table(cfg)
    niche = pl.DataFrame(
        {"jf_code": [jf.code for jf in cfg.job_families], "niche_share": [jf.niche_share for jf in cfg.job_families]}
    )

    snap = (
        grid.join(req, on="req_idx", how="left")
        .join(fills, on=["req_idx", "snapshot_day"], how="left")
        .join(seat_events, on=["req_idx", "snapshot_day"], how="left")
        .join(evidence, on=["req_idx", "snapshot_day"], how="left")
        .join(late_active, on=["req_idx", "snapshot_day"], how="left")
        .join(niche, on="jf_code", how="left")
        .with_columns(pl.col("fills", "active_total", "declines_recent", "reneges_recent", "late_active").fill_null(0))
        .with_columns(
            partial_applied=(pl.col("partial_day_sim") != NO_DAY)
            & (pl.col("partial_day_sim") <= pl.col("snapshot_day")),
            cancel_applied=(pl.col("cancel_day_sim") != NO_DAY) & (pl.col("cancel_day_sim") <= pl.col("snapshot_day")),
            rebase1=(pl.col("rebase1_day") != NO_DAY) & (pl.col("rebase1_day") <= pl.col("snapshot_day")),
            rebase2=(pl.col("rebase2_day") != NO_DAY) & (pl.col("rebase2_day") <= pl.col("snapshot_day")),
        )
        .with_columns(
            cancelled_positions=(
                pl.when(pl.col("partial_applied")).then(pl.col("partial_seats_sim")).otherwise(0)
                + pl.when(pl.col("cancel_applied")).then(pl.col("cancel_seats")).otherwise(0)
            ),
            is_full_cancelled=pl.col("cancel_applied") & (pl.col("cancel_kind") == "full"),
        )
        .with_columns(requested=pl.col("requested_positions") - pl.col("cancelled_positions"))
        .with_columns(
            openings=pl.max_horizontal(pl.col("requested") - pl.col("fills"), pl.lit(0)),
        )
        .with_columns(
            status=pl.when(pl.col("is_full_cancelled"))
            .then(pl.lit("cancelled"))
            .when(pl.col("openings") == 0)
            .then(pl.lit("filled"))
            .otherwise(pl.lit("open")),
            shift=pl.when(pl.col("rebase1")).then(pl.col("rebase1_shift")).otherwise(0)
            + pl.when(pl.col("rebase2")).then(pl.col("rebase2_shift")).otherwise(0),
        )
        .with_columns(
            thd=pl.min_horizontal(pl.col("thd_day") + pl.col("shift"), pl.lit(thd_end)),
        )
        .with_columns(toad=pl.min_horizontal(pl.col("toad_day") + pl.col("shift"), pl.col("thd")))
    )

    # ------------------------------------------------------------ keep window
    closed_since = pl.max_horizontal(
        pl.col("last_fill_day").fill_null(pl.col("approval_day")),
        pl.when(pl.col("partial_applied")).then(pl.col("partial_day_sim")).otherwise(pl.col("approval_day")),
        pl.when(pl.col("cancel_applied")).then(pl.col("cancel_day_sim")).otherwise(pl.col("approval_day")),
    )
    snap = snap.with_columns(closed_since=closed_since).filter(
        (pl.col("status") == "open")
        | (pl.col("snapshot_day") - pl.col("closed_since") <= keep_days)
        | (pl.col("snapshot_day") == as_of_day)
    )

    # ------------------------------------------------------------ constraint
    seats_open = pl.col("openings")
    past_toad = pl.col("snapshot_day") > pl.col("toad")
    days_open = pl.col("snapshot_day") - pl.col("approval_day")
    thin = pl.col("active_total") < 2 * seats_open
    stuck = pl.col("late_active") >= seats_open
    u, u2 = pl.col("u_constraint"), pl.col("u_constraint2")
    snap = (
        snap.join(jf_table, on="jf_code", how="inner")
        .filter((pl.col("u_constraint2") >= pl.col("lo")) & (pl.col("u_constraint2") < pl.col("hi")))
        .with_columns(
            open_constraint=pl.when(pl.col("status") != "open")
            .then(pl.lit(None, dtype=pl.Utf8))
            .when(~past_toad & (days_open < 45))
            .then(pl.when(u < 0.85).then(pl.lit("no_material_constraint")).otherwise(pl.col("generic_constraint")))
            .when(~past_toad)
            .then(pl.when(u < 0.60).then(pl.lit("no_material_constraint")).otherwise(pl.col("generic_constraint")))
            .when((pl.col("reneges_recent") > 0) | (pl.col("declines_recent") > 0))
            .then(pl.when(u2 < 0.6).then(pl.lit("compensation")).otherwise(pl.lit("candidate_availability")))
            .when(thin)
            .then(
                pl.when(u2 < pl.col("niche_share"))
                .then(pl.lit("niche_skills"))
                .otherwise(pl.lit("qualified_candidates"))
            )
            .when(stuck)
            .then(pl.when(u2 < 0.55).then(pl.lit("hiring_manager_delay")).otherwise(pl.lit("interview_capacity")))
            .otherwise(pl.when(u < 0.75).then(pl.col("generic_constraint")).otherwise(pl.lit("no_material_constraint")))
        )
        .sort(["req_idx", "snapshot_day"])
        .with_columns(
            hiring_constraint_code=pl.when(pl.col("status") == "cancelled")
            .then(pl.lit("no_material_constraint"))
            .otherwise(pl.col("open_constraint").forward_fill().over("req_idx"))
            .fill_null("no_material_constraint")
        )
    )

    # ------------------------------------------------------------ last source change
    changed = pl.max_horizontal(
        pl.col("approval_day"),
        pl.when(pl.col("rebase1")).then(pl.col("rebase1_day")).otherwise(pl.lit(NO_DAY)),
        pl.when(pl.col("rebase2")).then(pl.col("rebase2_day")).otherwise(pl.lit(NO_DAY)),
        pl.when(pl.col("partial_applied")).then(pl.col("partial_day_sim")).otherwise(pl.lit(NO_DAY)),
        pl.when(pl.col("cancel_applied")).then(pl.col("cancel_day_sim")).otherwise(pl.lit(NO_DAY)),
        pl.col("last_seat_event_day").fill_null(NO_DAY),
    )
    snap = (
        snap.with_columns(changed_day=changed)
        .with_columns(
            constraint_moved=pl.col("hiring_constraint_code")
            != pl.col("hiring_constraint_code").shift(1).over("req_idx")
        )
        .with_columns(
            # a constraint the recruiter re-recorded is a source edit too; the extract only
            # knows it by this snapshot date
            last_change_day=pl.when(pl.col("constraint_moved") & (pl.col("snapshot_day") > pl.col("changed_day")))
            .then(pl.col("snapshot_day"))
            .otherwise(pl.col("changed_day"))
        )
        # a record's last modification never moves backwards as later extracts are taken
        .with_columns(last_change_day=pl.col("last_change_day").cum_max().over("req_idx"))
    )

    return snap.select(
        "req_idx",
        "snapshot_day",
        "requisition_id",
        "requisition_title",
        "bu_code",
        "jf_code",
        "level_code",
        "work_location",
        "hiring_manager_id",
        "recruiter_id",
        "last_change_day",
        requisition_status=pl.col("status"),
        approval_day=pl.col("approval_day"),
        thd_day_snapshot=pl.col("thd"),
        toad_day_snapshot=pl.col("toad"),
        requested_positions=pl.col("requested"),
        openings_position=pl.col("openings"),
        cancelled_positions=pl.col("cancelled_positions"),
        hiring_constraint_code=pl.col("hiring_constraint_code"),
    ).sort(["snapshot_day", "requisition_id"])
