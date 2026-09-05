"""Source-level validation of the generated raw files.

These checks prove the *source* is internally consistent: keys, referential integrity,
date order, no actual event after the as-of date, one open stage per active application,
offer versions consistent with application status, HR starts only for accepted offers,
and the position identity on every requisition snapshot. They intentionally re-derive a
few quantities (active fills, losses) from dated events to check the snapshot numbers,
but never write those derivations to the outputs. Analytics rules (fill flags, risk
bands, cohorts, yields) are dbt's tests, not these.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .config import GeneratorConfig
from .funnel import STAGES

APPLICATION_STATUSES = [
    "active",
    "rejected",
    "withdrawn",
    "offer_declined",
    "offer_withdrawn",
    "offer_accepted",
    "offer_rescinded",
    "candidate_renege",
]
ACCEPTED_STATUSES = ["offer_accepted", "offer_rescinded", "candidate_renege"]
OFFER_STATUSES = ["extended", "superseded", "accepted", "declined", "withdrawn", "rescinded", "reneged"]
REQUISITION_STATUSES = ["open", "filled", "cancelled"]


@dataclass
class CheckResult:
    name: str
    failures: int
    sample: pl.DataFrame | None = None

    @property
    def passed(self) -> bool:
        return self.failures == 0


class Validator:
    def __init__(self, tables: dict[str, pl.DataFrame], cfg: GeneratorConfig) -> None:
        self.t = tables
        self.cfg = cfg
        self.as_of = cfg.dates.as_of
        self.results: list[CheckResult] = []

    # ------------------------------------------------------------------ helpers
    def _fail(self, name: str, bad: pl.DataFrame) -> None:
        self.results.append(CheckResult(name, bad.height, bad.head(5) if bad.height else None))

    def _expect_empty(self, name: str, frame: pl.DataFrame, condition: pl.Expr) -> None:
        self._fail(name, frame.filter(condition))

    def _unique(self, name: str, frame: pl.DataFrame, cols: list[str]) -> None:
        dup = frame.group_by(cols).len().filter(pl.col("len") > 1)
        self._fail(name, dup)

    def _fk(self, name: str, child: pl.DataFrame, parent: pl.DataFrame, col: str) -> None:
        bad = child.join(parent.select(col).unique(), on=col, how="anti")
        self._fail(name, bad)

    # ------------------------------------------------------------------ derived views
    def _latest_snapshot(self) -> pl.DataFrame:
        snap = self.t["ats_requisition_snapshot"]
        return (
            snap.filter(pl.col("snapshot_date") <= self.as_of)
            .sort(["requisition_id", "snapshot_date"])
            .group_by("requisition_id", maintain_order=True)
            .agg(pl.all().last())
        )

    def _accepted_cycles(self) -> pl.DataFrame:
        """One row per (application, offer cycle) with the earliest acceptance and any loss."""
        ov = self.t["ats_offer_version"]
        return ov.group_by("application_id", "offer_id").agg(
            accepted_date=pl.col("offer_accepted_date").min(),
            rescinded_date=pl.col("offer_rescinded_date").max(),
            renege_date=pl.col("candidate_renege_date").max(),
            accepted_versions=pl.col("offer_accepted_date").is_not_null().sum(),
        )

    def _active_fills_by_requisition(self) -> pl.DataFrame:
        cycles = self._accepted_cycles().filter(pl.col("accepted_date").is_not_null())
        loss = pl.coalesce(pl.col("rescinded_date"), pl.col("renege_date"))
        apps = self.t["ats_application"].select("application_id", "requisition_id")
        return (
            cycles.with_columns(is_lost=loss.is_not_null())
            .join(apps, on="application_id", how="left")
            .group_by("requisition_id")
            .agg(
                accepted_cycles=pl.len(),
                active_fills=(~pl.col("is_lost")).sum(),
                lost_after_acceptance=pl.col("is_lost").sum(),
            )
        )

    # ------------------------------------------------------------------ checks
    def run(self) -> list[CheckResult]:
        t = self.t
        snap, app, stg, ov, hr = (
            t["ats_requisition_snapshot"],
            t["ats_application"],
            t["ats_stage_history"],
            t["ats_offer_version"],
            t["hr_worker_event"],
        )
        latest = self._latest_snapshot()
        as_of = pl.lit(self.as_of)

        # keys ----------------------------------------------------------------
        self._unique("snapshot: unique (requisition_id, snapshot_date)", snap, ["requisition_id", "snapshot_date"])
        self._unique("application: unique application_id", app, ["application_id"])
        self._unique("application: unique (candidate_id, requisition_id)", app, ["candidate_id", "requisition_id"])
        self._unique("stage_history: unique stage_history_id", stg, ["stage_history_id"])
        self._unique(
            "stage_history: unique (application_id, stage_sequence)", stg, ["application_id", "stage_sequence"]
        )
        self._unique(
            "stage_history: unique (application_id, stage_code, entered_date)",
            stg,
            ["application_id", "stage_code", "stage_entered_date"],
        )
        self._unique("offer_version: unique offer_version_id", ov, ["offer_version_id"])
        self._unique("worker_event: unique worker_event_id", hr, ["worker_event_id"])
        for name in ("ats_business_unit", "ats_job_family", "ats_job_level"):
            code = {
                "ats_business_unit": "business_unit_code",
                "ats_job_family": "job_family_code",
                "ats_job_level": "job_level_code",
            }[name]
            self._unique(f"{name}: unique {code}", t[name], [code])

        # referential integrity --------------------------------------------------
        self._fk("snapshot: business_unit_code exists", snap, t["ats_business_unit"], "business_unit_code")
        self._fk("snapshot: job_family_code exists", snap, t["ats_job_family"], "job_family_code")
        self._fk("snapshot: job_level_code exists", snap, t["ats_job_level"], "job_level_code")
        self._fk("application: requisition_id exists in snapshots", app, snap, "requisition_id")
        self._fk("stage_history: application_id exists", stg, app, "application_id")
        self._fk("offer_version: application_id exists", ov, app, "application_id")
        self._fk("worker_event: application_id exists", hr, app, "application_id")
        self._expect_empty(
            "offer_version: requisition matches application",
            ov.join(app.select("application_id", app_req=pl.col("requisition_id")), on="application_id"),
            pl.col("requisition_id") != pl.col("app_req"),
        )

        # vocabularies -------------------------------------------------------------
        self._expect_empty(
            "snapshot: requisition_status vocabulary", snap, ~pl.col("requisition_status").is_in(REQUISITION_STATUSES)
        )
        self._expect_empty(
            "snapshot: hiring constraint vocabulary",
            snap,
            ~pl.col("primary_hiring_constraint").is_in(self.cfg.hiring_constraints),
        )
        self._expect_empty(
            "application: status vocabulary", app, ~pl.col("application_status").is_in(APPLICATION_STATUSES)
        )
        self._expect_empty("stage_history: stage vocabulary", stg, ~pl.col("stage_code").is_in(STAGES))
        self._expect_empty("offer_version: status vocabulary", ov, ~pl.col("offer_status").is_in(OFFER_STATUSES))
        self._expect_empty(
            "worker_event: event_type vocabulary", hr, ~pl.col("event_type").is_in(["hire", "termination"])
        )

        # no actual event after the as-of date -----------------------------------------
        self._expect_empty(
            "snapshot: snapshot_date and approval_date <= as_of",
            snap,
            (pl.col("snapshot_date") > as_of) | (pl.col("approval_date") > as_of),
        )
        self._expect_empty(
            "application: application_date and status_date <= as_of",
            app,
            (pl.col("application_date") > as_of) | (pl.col("status_date") > as_of),
        )
        self._expect_empty(
            "stage_history: dates <= as_of",
            stg,
            (pl.col("stage_entered_date") > as_of) | (pl.col("stage_exited_date") > as_of),
        )
        self._expect_empty(
            "offer_version: actual dates <= as_of",
            ov,
            (pl.col("offer_extended_date") > as_of)
            | (pl.col("offer_accepted_date") > as_of)
            | (pl.col("offer_declined_date") > as_of)
            | (pl.col("offer_withdrawn_date") > as_of)
            | (pl.col("offer_rescinded_date") > as_of)
            | (pl.col("candidate_renege_date") > as_of),
        )
        self._expect_empty(
            "worker_event: event_date and record_created_date <= as_of",
            hr,
            (pl.col("event_date") > as_of) | (pl.col("record_created_date") > as_of),
        )
        self._expect_empty(
            "snapshot: target dates within planning horizon",
            snap,
            (pl.col("target_hire_date") > pl.lit(self.cfg.dates.future_thd_end))
            | (pl.col("target_hire_date") < pl.col("approval_date")),
        )
        self._expect_empty(
            "application: application_date >= history_start",
            app,
            pl.col("application_date") < pl.lit(self.cfg.dates.history_start),
        )

        # requisition snapshot rules --------------------------------------------------
        self._expect_empty(
            "snapshot: TOAD between approval and THD",
            snap,
            (pl.col("target_offer_acceptance_date") > pl.col("target_hire_date"))
            | (pl.col("target_offer_acceptance_date") < pl.col("approval_date")),
        )
        self._expect_empty(
            "snapshot: quantities non-negative",
            snap,
            (pl.col("requested_positions") < 0)
            | (pl.col("openings_position") < 0)
            | (pl.col("cancelled_positions") < 0),
        )
        self._expect_empty(
            "snapshot: openings <= requested", snap, pl.col("openings_position") > pl.col("requested_positions")
        )
        self._expect_empty(
            "snapshot: open status means openings > 0",
            snap,
            (pl.col("requisition_status") == "open") & (pl.col("openings_position") == 0),
        )
        self._expect_empty(
            "snapshot: filled status means openings = 0 and requested > 0",
            snap,
            (pl.col("requisition_status") == "filled")
            & ((pl.col("openings_position") != 0) | (pl.col("requested_positions") == 0)),
        )
        self._expect_empty(
            "snapshot: cancelled status means requested = 0 and openings = 0",
            snap,
            (pl.col("requisition_status") == "cancelled")
            & ((pl.col("openings_position") != 0) | (pl.col("requested_positions") != 0)),
        )
        self._expect_empty(
            "snapshot: original seats = requested + cancelled is stable per requisition",
            snap.with_columns(total=pl.col("requested_positions") + pl.col("cancelled_positions"))
            .group_by("requisition_id")
            .agg(pl.col("total").n_unique().alias("n")),
            pl.col("n") > 1,
        )
        self._expect_empty(
            "snapshot: cancelled_positions never decreases",
            snap.sort(["requisition_id", "snapshot_date"]).with_columns(
                prev=pl.col("cancelled_positions").shift(1).over("requisition_id")
            ),
            pl.col("prev").is_not_null() & (pl.col("cancelled_positions") < pl.col("prev")),
        )
        self._expect_empty(
            "snapshot: every requisition has a snapshot on or before as_of", latest, pl.col("snapshot_date").is_null()
        )
        self._expect_empty(
            "snapshot: THD never moves earlier across snapshots",
            snap.sort(["requisition_id", "snapshot_date"]).with_columns(
                prev=pl.col("target_hire_date").shift(1).over("requisition_id")
            ),
            pl.col("prev").is_not_null() & (pl.col("target_hire_date") < pl.col("prev")),
        )

        # snapshot identity against dated events ------------------------------------------
        fills = self._active_fills_by_requisition()
        recon = latest.join(fills, on="requisition_id", how="left").with_columns(
            pl.col("active_fills", "accepted_cycles", "lost_after_acceptance").fill_null(0)
        )
        self._expect_empty(
            "reconciliation: requested = active fills + openings (latest snapshot, non-cancelled)",
            recon,
            (pl.col("requisition_status") != "cancelled")
            & (pl.col("requested_positions") != pl.col("active_fills") + pl.col("openings_position")),
        )
        self._expect_empty(
            "reconciliation: cancelled requisitions hold no active fill",
            recon,
            (pl.col("requisition_status") == "cancelled") & (pl.col("active_fills") > 0),
        )

        # applications and stage history ------------------------------------------------
        app_req = app.join(
            latest.select("requisition_id", latest_status=pl.col("requisition_status")), on="requisition_id", how="left"
        )
        self._expect_empty(
            "application: active applications belong to open requisitions",
            app_req,
            (pl.col("application_status") == "active") & (pl.col("latest_status") != "open"),
        )
        self._expect_empty(
            "application: status_date >= application_date", app, pl.col("status_date") < pl.col("application_date")
        )
        self._expect_empty(
            "application: application_date >= requisition approval",
            app.join(latest.select("requisition_id", "approval_date"), on="requisition_id", how="left"),
            pl.col("application_date") < pl.col("approval_date"),
        )
        self._expect_empty(
            "application: disposition_reason only on rejected/withdrawn",
            app,
            (
                pl.col("disposition_reason").is_not_null()
                & ~pl.col("application_status").is_in(["rejected", "withdrawn"])
            )
            | (pl.col("disposition_reason").is_null() & pl.col("application_status").is_in(["rejected", "withdrawn"])),
        )

        stg_app = stg.join(
            app.select("application_id", "application_status", "application_date", "current_stage_code"),
            on="application_id",
            how="left",
        )
        self._expect_empty(
            "stage_history: exit >= entry", stg, pl.col("stage_exited_date") < pl.col("stage_entered_date")
        )
        self._expect_empty(
            "stage_history: first stage is review on the application date",
            stg_app,
            (pl.col("stage_sequence") == 1)
            & ((pl.col("stage_code") != "review") | (pl.col("stage_entered_date") != pl.col("application_date"))),
        )
        order = {code: i for i, code in enumerate(STAGES)}
        self._expect_empty(
            "stage_history: stages follow the governed order without skips",
            stg.with_columns(stage_index=pl.col("stage_code").replace_strict(order, return_dtype=pl.Int64)),
            pl.col("stage_index") != pl.col("stage_sequence") - 1,
        )
        chain = stg.sort(["application_id", "stage_sequence"]).with_columns(
            prev_exit=pl.col("stage_exited_date").shift(1).over("application_id")
        )
        self._expect_empty(
            "stage_history: entry of stage n+1 equals exit of stage n",
            chain,
            (pl.col("stage_sequence") > 1) & (pl.col("prev_exit") != pl.col("stage_entered_date")),
        )
        open_rows = stg.filter(pl.col("stage_exited_date").is_null()).group_by("application_id").len()
        self._expect_empty("stage_history: at most one open stage per application", open_rows, pl.col("len") > 1)
        open_vs_status = app.join(
            open_rows.rename({"len": "open_stages"}), on="application_id", how="left"
        ).with_columns(pl.col("open_stages").fill_null(0))
        self._expect_empty(
            "stage_history: open stage if and only if application is active",
            open_vs_status,
            (pl.col("application_status") == "active") != (pl.col("open_stages") == 1),
        )
        last_stage = (
            stg.sort(["application_id", "stage_sequence"])
            .group_by("application_id", maintain_order=True)
            .agg(last=pl.col("stage_code").last())
        )
        self._expect_empty(
            "application: current_stage_code equals last stage entered",
            app.join(last_stage, on="application_id", how="left"),
            pl.col("current_stage_code") != pl.col("last"),
        )
        self._expect_empty(
            "application: accepted / declined / withdrawn-offer statuses end in the offer stage",
            app,
            pl.col("application_status").is_in(ACCEPTED_STATUSES + ["offer_declined", "offer_withdrawn"])
            & (pl.col("current_stage_code") != "offer"),
        )
        self._expect_empty(
            "stage_history: every application has a stage history",
            app.join(stg.select("application_id").unique(), on="application_id", how="anti"),
            pl.lit(True),
        )

        # offers --------------------------------------------------------------------------
        cycles = self._accepted_cycles()
        by_app = cycles.group_by("application_id").agg(
            accepted_any=pl.col("accepted_date").is_not_null().any(),
            n_cycles=pl.len(),
            rescinded_any=pl.col("rescinded_date").is_not_null().any(),
            reneged_any=pl.col("renege_date").is_not_null().any(),
        )
        app_ov = app.join(by_app, on="application_id", how="left").with_columns(
            pl.col("accepted_any", "rescinded_any", "reneged_any").fill_null(False), pl.col("n_cycles").fill_null(0)
        )
        self._expect_empty(
            "offers: accepted-family statuses have an accepted version",
            app_ov,
            pl.col("application_status").is_in(ACCEPTED_STATUSES) & ~pl.col("accepted_any"),
        )
        self._expect_empty(
            "offers: non-accepted statuses have no accepted version",
            app_ov,
            ~pl.col("application_status").is_in(ACCEPTED_STATUSES) & pl.col("accepted_any"),
        )
        self._expect_empty(
            "offers: offer_rescinded status has a rescinded version",
            app_ov,
            (pl.col("application_status") == "offer_rescinded") & ~pl.col("rescinded_any"),
        )
        self._expect_empty(
            "offers: candidate_renege status has a reneged version",
            app_ov,
            (pl.col("application_status") == "candidate_renege") & ~pl.col("reneged_any"),
        )
        self._expect_empty(
            "offers: offer_accepted status has no post-acceptance loss",
            app_ov,
            (pl.col("application_status") == "offer_accepted") & (pl.col("rescinded_any") | pl.col("reneged_any")),
        )
        self._expect_empty(
            "offers: applications that reached the offer stage have an offer",
            app_ov,
            (pl.col("current_stage_code") == "offer") & (pl.col("n_cycles") == 0),
        )
        self._expect_empty(
            "offers: applications that never reached the offer stage have no offer",
            app_ov,
            (pl.col("current_stage_code") != "offer") & (pl.col("n_cycles") > 0),
        )
        ov_app = ov.join(app.select("application_id", "application_date"), on="application_id", how="left")
        self._expect_empty(
            "offer_version: extended >= application date",
            ov_app,
            pl.col("offer_extended_date") < pl.col("application_date"),
        )
        self._expect_empty(
            "offer_version: response dates >= extended",
            ov,
            (pl.col("offer_accepted_date") < pl.col("offer_extended_date"))
            | (pl.col("offer_declined_date") < pl.col("offer_extended_date"))
            | (pl.col("offer_withdrawn_date") < pl.col("offer_extended_date")),
        )
        self._expect_empty(
            "offer_version: rescind / renege dates >= accepted",
            ov,
            (pl.col("offer_rescinded_date") < pl.col("offer_accepted_date"))
            | (pl.col("candidate_renege_date") < pl.col("offer_accepted_date")),
        )
        self._expect_empty(
            "offer_version: post-acceptance loss only on accepted versions",
            ov,
            (pl.col("offer_rescinded_date").is_not_null() | pl.col("candidate_renege_date").is_not_null())
            & pl.col("offer_accepted_date").is_null(),
        )
        self._expect_empty(
            "offer_version: accepted and declined/withdrawn are exclusive per version",
            ov,
            pl.col("offer_accepted_date").is_not_null()
            & (pl.col("offer_declined_date").is_not_null() | pl.col("offer_withdrawn_date").is_not_null()),
        )
        self._expect_empty(
            "offer_version: status agrees with dates",
            ov,
            ((pl.col("offer_status") == "accepted") & pl.col("offer_accepted_date").is_null())
            | ((pl.col("offer_status") == "declined") & pl.col("offer_declined_date").is_null())
            | ((pl.col("offer_status") == "withdrawn") & pl.col("offer_withdrawn_date").is_null())
            | ((pl.col("offer_status") == "rescinded") & pl.col("offer_rescinded_date").is_null())
            | ((pl.col("offer_status") == "reneged") & pl.col("candidate_renege_date").is_null())
            | (pl.col("offer_status").is_in(["extended", "superseded"]) & pl.col("offer_accepted_date").is_not_null()),
        )
        self._expect_empty(
            "offer_version: exactly one current version per offer",
            ov.group_by("offer_id").agg(n=pl.col("is_current_version").sum()),
            pl.col("n") != 1,
        )
        self._expect_empty(
            "offer_version: version numbers are contiguous from 1",
            ov.group_by("offer_id").agg(
                n=pl.len(), mx=pl.col("offer_version_number").max(), mn=pl.col("offer_version_number").min()
            ),
            (pl.col("mn") != 1) | (pl.col("mx") != pl.col("n")),
        )
        self._expect_empty(
            "offer_version: acceptance not before the offer stage was entered",
            ov.join(
                stg.filter(pl.col("stage_code") == "offer").select("application_id", "stage_entered_date"),
                on="application_id",
                how="left",
            ),
            pl.col("offer_accepted_date") < pl.col("stage_entered_date"),
        )

        # HR --------------------------------------------------------------------------------
        hires = hr.filter(pl.col("event_type") == "hire")
        terms = hr.filter(pl.col("event_type") == "termination")
        first_acc = (
            cycles.filter(pl.col("accepted_date").is_not_null())
            .group_by("application_id")
            .agg(accepted_date=pl.col("accepted_date").min())
        )
        hires_acc = hires.join(first_acc, on="application_id", how="left").join(
            app.select("application_id", "application_status"), on="application_id", how="left"
        )
        self._expect_empty(
            "worker_event: hire has an accepted offer that was not lost",
            hires_acc,
            pl.col("accepted_date").is_null() | (pl.col("application_status") != "offer_accepted"),
        )
        self._expect_empty(
            "worker_event: hire date >= offer accepted date", hires_acc, pl.col("event_date") < pl.col("accepted_date")
        )
        self._unique(
            "worker_event: one worker per application (after de-duplication)",
            hires.select("application_id", "worker_id", "event_date").unique(),
            ["application_id"],
        )
        hire_dates = hires.group_by("worker_id").agg(start=pl.col("event_date").min())
        self._expect_empty(
            "worker_event: termination >= hire",
            terms.join(hire_dates, on="worker_id", how="left"),
            pl.col("start").is_null() | (pl.col("event_date") < pl.col("start")),
        )
        self._expect_empty(
            "worker_event: terminated workers have a hire event",
            terms.join(hires.select("worker_id").unique(), on="worker_id", how="anti"),
            pl.lit(True),
        )
        self._expect_empty(
            "worker_event: record_created_date >= event_date", hr, pl.col("record_created_date") < pl.col("event_date")
        )
        return self.results


def run_validations(tables: dict[str, pl.DataFrame], cfg: GeneratorConfig) -> list[CheckResult]:
    return Validator(tables, cfg).run()


def format_results(results: list[CheckResult]) -> str:
    lines = []
    for r in results:
        mark = "PASS" if r.passed else "FAIL"
        lines.append(f"[{mark}] {r.name}" + ("" if r.passed else f"  ({r.failures} rows)"))
    failed = [r for r in results if not r.passed]
    lines.append(f"{len(results) - len(failed)}/{len(results)} checks passed")
    return "\n".join(lines)
