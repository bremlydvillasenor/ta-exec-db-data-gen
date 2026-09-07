"""Source-level validation of the generated raw files.

These checks prove the *source* is internally consistent: unique keys within the extract,
referential integrity, date order, no actual event after the as-of date, the raw
`updated_at` / `extracted_at` rules, one open stage per active application, current offer
rows consistent with application status, HR starts only for accepted offers that were not
lost, and the position identity on every requisition snapshot. They intentionally
re-derive a few quantities (active fills, losses) from dated events to check the snapshot
numbers, but never write those derivations to the outputs. Analytics rules (fill flags,
risk bands, cohorts, yields) are dbt's tests, not these.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .config import GeneratorConfig
from .funnel import STAGES
from .offers import OFFER_STATUSES
from .timestamps import business_cutoff

APPLICATION_STATUSES = [
    "active",
    "rejected",
    "withdrawn",
    "offer_declined",
    "offer_withdrawn",
    "offer_accepted",
    "started",
    "offer_rescinded",
    "candidate_renege",
]
# statuses that imply a preserved offer-acceptance event
ACCEPTED_STATUSES = ["offer_accepted", "started", "offer_rescinded", "candidate_renege"]
OFFER_STAGE_STATUSES = ACCEPTED_STATUSES + ["offer_declined", "offer_withdrawn"]
STAGE_EXIT_REASONS = ["rejected", "withdrawn", "offer_declined", "offer_withdrawn"]
REQUISITION_STATUSES = ["open", "filled", "cancelled"]
LOOKUP_CODES = {
    "ats_business_unit": "business_unit_code",
    "ats_job_family": "job_family_code",
    "ats_job_level": "job_level_code",
}

# Declared shape of every raw file: the column, its kind, and whether a null is allowed.
# "text" / "date" / "integer" / "boolean" / "timestamp" are checked against the loaded dtype,
# so a column that arrives as the wrong type - or entirely empty - fails here rather than
# slipping through a comparison that silently evaluates to null.
TEXT, DATE, INTEGER, BOOLEAN, TIMESTAMP = "text", "date", "integer", "boolean", "timestamp"
REQUIRED, NULLABLE = True, False

SCHEMA: dict[str, dict[str, tuple[str, bool]]] = {
    "ats_business_unit": {
        "business_unit_code": (TEXT, REQUIRED),
        "business_unit_name": (TEXT, REQUIRED),
        "sort_order": (INTEGER, REQUIRED),
        "is_active": (BOOLEAN, REQUIRED),
        "updated_at": (TIMESTAMP, REQUIRED),
        "extracted_at": (TIMESTAMP, REQUIRED),
    },
    "ats_job_family": {
        "job_family_code": (TEXT, REQUIRED),
        "job_family_name": (TEXT, REQUIRED),
        "sort_order": (INTEGER, REQUIRED),
        "is_active": (BOOLEAN, REQUIRED),
        "updated_at": (TIMESTAMP, REQUIRED),
        "extracted_at": (TIMESTAMP, REQUIRED),
    },
    "ats_job_level": {
        "job_level_code": (TEXT, REQUIRED),
        "job_level_name": (TEXT, REQUIRED),
        "level_rank": (INTEGER, REQUIRED),
        "is_active": (BOOLEAN, REQUIRED),
        "updated_at": (TIMESTAMP, REQUIRED),
        "extracted_at": (TIMESTAMP, REQUIRED),
    },
    "ats_requisition_snapshot": {
        "snapshot_date": (DATE, REQUIRED),
        "requisition_id": (TEXT, REQUIRED),
        "requisition_title": (TEXT, REQUIRED),
        "business_unit_code": (TEXT, REQUIRED),
        "job_family_code": (TEXT, REQUIRED),
        "job_level_code": (TEXT, REQUIRED),
        "work_location": (TEXT, REQUIRED),
        "hiring_manager_id": (TEXT, REQUIRED),
        "recruiter_id": (TEXT, REQUIRED),
        "requisition_status": (TEXT, REQUIRED),
        "approval_date": (DATE, REQUIRED),
        "target_hire_date": (DATE, REQUIRED),
        "target_offer_acceptance_date": (DATE, REQUIRED),
        "requested_positions": (INTEGER, REQUIRED),
        "openings_position": (INTEGER, REQUIRED),
        "cancelled_positions": (INTEGER, REQUIRED),
        "hiring_constraint_code": (TEXT, REQUIRED),
        "updated_at": (TIMESTAMP, REQUIRED),
        "extracted_at": (TIMESTAMP, REQUIRED),
    },
    "ats_application": {
        "application_id": (TEXT, REQUIRED),
        "candidate_id": (TEXT, REQUIRED),
        "requisition_id": (TEXT, REQUIRED),
        "application_date": (DATE, REQUIRED),
        "source_channel": (TEXT, REQUIRED),
        "application_status_current": (TEXT, REQUIRED),
        "current_stage_code": (TEXT, REQUIRED),
        "rejected_date": (DATE, NULLABLE),
        "withdrawal_date": (DATE, NULLABLE),
        "disposition_reason": (TEXT, NULLABLE),
        "updated_at": (TIMESTAMP, REQUIRED),
        "extracted_at": (TIMESTAMP, REQUIRED),
    },
    "ats_stage_history": {
        "stage_event_id": (TEXT, REQUIRED),
        "application_id": (TEXT, REQUIRED),
        "stage_code": (TEXT, REQUIRED),
        "stage_sequence_number": (INTEGER, REQUIRED),
        "stage_entry_date": (DATE, REQUIRED),
        "stage_exit_date": (DATE, NULLABLE),
        "exit_reason": (TEXT, NULLABLE),
        "updated_at": (TIMESTAMP, REQUIRED),
        "extracted_at": (TIMESTAMP, REQUIRED),
    },
    "ats_offer": {
        "application_id": (TEXT, REQUIRED),
        "requisition_id": (TEXT, REQUIRED),
        "offer_status_current": (TEXT, REQUIRED),
        "offer_extended_date": (DATE, REQUIRED),
        "offer_accepted_date": (DATE, NULLABLE),
        "offer_declined_date": (DATE, NULLABLE),
        "offer_withdrawn_date": (DATE, NULLABLE),
        "offer_rescinded_date": (DATE, NULLABLE),
        "candidate_renege_date": (DATE, NULLABLE),
        "planned_start_date": (DATE, NULLABLE),
        "base_salary": (INTEGER, REQUIRED),
        "currency": (TEXT, REQUIRED),
        "updated_at": (TIMESTAMP, REQUIRED),
        "extracted_at": (TIMESTAMP, REQUIRED),
    },
    "hr_worker_event": {
        "worker_event_id": (TEXT, REQUIRED),
        "worker_id": (TEXT, REQUIRED),
        "candidate_id": (TEXT, REQUIRED),
        "application_id": (TEXT, REQUIRED),
        "requisition_id": (TEXT, REQUIRED),
        "event_type": (TEXT, REQUIRED),
        "event_date": (DATE, REQUIRED),
        "termination_reason": (TEXT, NULLABLE),
        "updated_at": (TIMESTAMP, REQUIRED),
        "extracted_at": (TIMESTAMP, REQUIRED),
    },
}


def _kind_matches(dtype: pl.DataType, kind: str) -> bool:
    if kind == TEXT:
        return dtype == pl.Utf8
    if kind == DATE:
        return dtype == pl.Date
    if kind == INTEGER:
        return dtype.is_integer()
    if kind == BOOLEAN:
        return dtype == pl.Boolean
    return isinstance(dtype, pl.Datetime)


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

    def _accepted_offers(self) -> pl.DataFrame:
        """One row per application holding an acceptance event, with any post-acceptance loss.

        Contract 1.3 keeps one current offer row per application, so this is a filter, not a
        resolution step: there is nothing to collapse and no cycle to choose between.
        """
        off = self.t["ats_offer"]
        return off.filter(pl.col("offer_accepted_date").is_not_null()).select(
            "application_id",
            "requisition_id",
            "offer_status_current",
            accepted_date=pl.col("offer_accepted_date"),
            rescinded_date=pl.col("offer_rescinded_date"),
            renege_date=pl.col("candidate_renege_date"),
            is_lost=pl.col("offer_rescinded_date").is_not_null() | pl.col("candidate_renege_date").is_not_null(),
        )

    def _active_fills_by_requisition(self) -> pl.DataFrame:
        return (
            self._accepted_offers()
            .group_by("requisition_id")
            .agg(
                accepted_offer_events=pl.len(),
                active_fills=(~pl.col("is_lost")).sum(),
                lost_after_acceptance=pl.col("is_lost").sum(),
            )
        )

    # ------------------------------------------------------------------ checks
    def run(self) -> list[CheckResult]:
        t = self.t
        snap, app, stg, off, hr = (
            t["ats_requisition_snapshot"],
            t["ats_application"],
            t["ats_stage_history"],
            t["ats_offer"],
            t["hr_worker_event"],
        )
        latest = self._latest_snapshot()
        as_of = pl.lit(self.as_of)

        # declared shape: columns, data types, and required values ------------------
        # This runs first and is a precondition, not one check among many. Every rule below
        # assumes the declared columns exist with the declared type; comparing a date against
        # a string raises, and comparing anything against a missing value quietly yields null.
        # So if the shape is wrong, report that and stop rather than produce noise or crash.
        self._check_schema()
        if any(not r.passed for r in self.results):
            return self.results

        # keys ----------------------------------------------------------------
        self._unique("snapshot: unique (requisition_id, snapshot_date)", snap, ["requisition_id", "snapshot_date"])
        self._unique("application: unique application_id", app, ["application_id"])
        self._unique("stage_history: unique stage_event_id", stg, ["stage_event_id"])
        self._unique(
            "stage_history: unique (application_id, stage_sequence_number)",
            stg,
            ["application_id", "stage_sequence_number"],
        )
        self._unique(
            "stage_history: unique (application_id, stage_code, entry_date)",
            stg,
            ["application_id", "stage_code", "stage_entry_date"],
        )
        self._unique("offer: unique application_id (one current offer per application)", off, ["application_id"])
        self._unique("worker_event: unique worker_event_id", hr, ["worker_event_id"])
        for name, code in LOOKUP_CODES.items():
            self._unique(f"{name}: unique {code}", t[name], [code])

        # referential integrity --------------------------------------------------
        self._fk("snapshot: business_unit_code exists", snap, t["ats_business_unit"], "business_unit_code")
        self._fk("snapshot: job_family_code exists", snap, t["ats_job_family"], "job_family_code")
        self._fk("snapshot: job_level_code exists", snap, t["ats_job_level"], "job_level_code")
        self._fk("application: requisition_id exists in snapshots", app, snap, "requisition_id")
        self._fk("stage_history: application_id exists", stg, app, "application_id")
        self._fk("offer: application_id exists", off, app, "application_id")
        self._fk("worker_event: application_id exists", hr, app, "application_id")
        self._expect_empty(
            "offer: requisition matches application",
            off.join(app.select("application_id", app_req=pl.col("requisition_id")), on="application_id"),
            pl.col("requisition_id") != pl.col("app_req"),
        )

        # vocabularies -------------------------------------------------------------
        self._expect_empty(
            "snapshot: requisition_status vocabulary", snap, ~pl.col("requisition_status").is_in(REQUISITION_STATUSES)
        )
        self._expect_empty(
            "snapshot: hiring constraint vocabulary",
            snap,
            ~pl.col("hiring_constraint_code").is_in(self.cfg.hiring_constraints),
        )
        self._expect_empty(
            "application: status vocabulary", app, ~pl.col("application_status_current").is_in(APPLICATION_STATUSES)
        )
        self._expect_empty("stage_history: stage vocabulary", stg, ~pl.col("stage_code").is_in(STAGES))
        self._expect_empty(
            "stage_history: exit_reason carries only pre-acceptance losses",
            stg,
            pl.col("exit_reason").is_not_null() & ~pl.col("exit_reason").is_in(STAGE_EXIT_REASONS),
        )
        self._expect_empty("offer: status vocabulary", off, ~pl.col("offer_status_current").is_in(OFFER_STATUSES))
        self._expect_empty(
            "worker_event: event_type vocabulary", hr, ~pl.col("event_type").is_in(["start", "termination"])
        )

        # raw timestamps ---------------------------------------------------------------
        self._check_timestamps()

        # no actual event after the as-of date -----------------------------------------
        self._expect_empty(
            "snapshot: snapshot_date and approval_date <= as_of",
            snap,
            (pl.col("snapshot_date") > as_of) | (pl.col("approval_date") > as_of),
        )
        self._expect_empty(
            "application: actual dates <= as_of",
            app,
            (pl.col("application_date") > as_of)
            | (pl.col("rejected_date") > as_of)
            | (pl.col("withdrawal_date") > as_of),
        )
        self._expect_empty(
            "stage_history: dates <= as_of",
            stg,
            (pl.col("stage_entry_date") > as_of) | (pl.col("stage_exit_date") > as_of),
        )
        self._expect_empty(
            "offer: actual dates <= as_of",
            off,
            (pl.col("offer_extended_date") > as_of)
            | (pl.col("offer_accepted_date") > as_of)
            | (pl.col("offer_declined_date") > as_of)
            | (pl.col("offer_withdrawn_date") > as_of)
            | (pl.col("offer_rescinded_date") > as_of)
            | (pl.col("candidate_renege_date") > as_of),
        )
        self._expect_empty("worker_event: event_date <= as_of", hr, pl.col("event_date") > as_of)
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
        self._check_snapshots(snap, latest)

        # applications and stage history ------------------------------------------------
        self._check_applications(app, stg, latest)

        # offers ---------------------------------------------------------------------------
        self._check_offers(app, stg, off)

        # HR -------------------------------------------------------------------------------
        self._check_hr(app, hr)

        # repeat attempts and candidate realism -------------------------------------------------
        self._check_repeat_attempts(app, stg)
        self._check_candidate_realism(app, hr)
        return self.results

    # ------------------------------------------------------------------ declared shape
    def _check_schema(self) -> None:
        """Every file has the declared columns, of the declared type, with no missing values.

        Comparison-based rules cannot do this job: in SQL and in Polars a comparison against
        null evaluates to null, so a required identifier, status or quantity that arrives
        empty slips silently through every range and consistency check downstream. This is
        the check that stops that, and it runs before any of them.
        """
        for name, columns in SCHEMA.items():
            frame = self.t.get(name)
            if frame is None:
                self._fail(f"{name}: table is present in the extract", pl.DataFrame({"missing_table": [name]}))
                continue
            missing = [c for c in columns if c not in frame.columns]
            self._fail(
                f"{name}: declared columns are present",
                pl.DataFrame({"missing_column": missing}) if missing else pl.DataFrame(),
            )
            unexpected = [c for c in frame.columns if c not in columns]
            self._fail(
                f"{name}: no undeclared columns",
                pl.DataFrame({"unexpected_column": unexpected}) if unexpected else pl.DataFrame(),
            )
            wrong_type = [
                {"column": col, "expected": kind, "actual": str(frame.schema[col])}
                for col, (kind, _) in columns.items()
                if col in frame.columns and not _kind_matches(frame.schema[col], kind)
            ]
            self._fail(
                f"{name}: columns have the declared data type",
                pl.DataFrame(wrong_type) if wrong_type else pl.DataFrame(),
            )
            required = [col for col, (_, req) in columns.items() if req and col in frame.columns]
            nulls = [
                {"column": col, "null_rows": frame[col].null_count()}
                for col in required
                if frame[col].null_count() > 0
            ]
            self._fail(
                f"{name}: required columns have no missing values",
                pl.DataFrame(nulls) if nulls else pl.DataFrame(),
            )

    # ------------------------------------------------------------------ timestamps
    def _check_timestamps(self) -> None:
        """Contract rules on the two raw metadata columns, on every file including lookups."""
        extracted_at = pl.lit(self.cfg.timestamps.extracted_at)
        cutoff = pl.lit(business_cutoff(self.cfg))
        for name, frame in self.t.items():
            self._expect_empty(
                f"{name}: updated_at and extracted_at present",
                frame,
                pl.col("updated_at").is_null() | pl.col("extracted_at").is_null(),
            )
            self._expect_empty(
                f"{name}: extracted_at is the configured batch value",
                frame,
                pl.col("extracted_at") != extracted_at,
            )
            self._expect_empty(
                f"{name}: updated_at <= extracted_at", frame, pl.col("updated_at") > pl.col("extracted_at")
            )
            self._expect_empty(
                f"{name}: updated_at within the synthetic business cutoff", frame, pl.col("updated_at") > cutoff
            )
        # an earlier snapshot may not carry a change it could not yet know about
        self._expect_empty(
            "snapshot: updated_at is known by the snapshot date",
            self.t["ats_requisition_snapshot"],
            pl.col("updated_at").dt.date() > pl.col("snapshot_date"),
        )
        self._expect_empty(
            "snapshot: updated_at never moves backwards for one requisition",
            self.t["ats_requisition_snapshot"]
            .sort(["requisition_id", "snapshot_date"])
            .with_columns(prev=pl.col("updated_at").shift(1).over("requisition_id")),
            pl.col("prev").is_not_null() & (pl.col("updated_at") < pl.col("prev")),
        )
        # `updated_at` has to reflect the LATEST recorded change on the row, not just the
        # first one. A record whose timestamp stops at its opening event looks unchanged to
        # an incremental load, so the later exit, acceptance or loss would never be picked up.
        self._expect_empty(
            "snapshot: updated_at is not before the approval date",
            self.t["ats_requisition_snapshot"],
            pl.col("updated_at").dt.date() < pl.col("approval_date"),
        )
        self._expect_empty(
            "application: updated_at reflects its latest recorded date",
            self.t["ats_application"],
            pl.col("updated_at").dt.date()
            < pl.max_horizontal("application_date", "rejected_date", "withdrawal_date"),
        )
        self._expect_empty(
            "stage_history: updated_at reflects its latest recorded date",
            self.t["ats_stage_history"],
            pl.col("updated_at").dt.date() < pl.max_horizontal("stage_entry_date", "stage_exit_date"),
        )
        self._expect_empty(
            "offer: updated_at reflects its latest recorded date",
            self.t["ats_offer"],
            pl.col("updated_at").dt.date()
            < pl.max_horizontal(
                "offer_extended_date",
                "offer_accepted_date",
                "offer_declined_date",
                "offer_withdrawn_date",
                "offer_rescinded_date",
                "candidate_renege_date",
            ),
        )
        self._expect_empty(
            "worker_event: updated_at is not before the event date",
            self.t["hr_worker_event"],
            pl.col("updated_at").dt.date() < pl.col("event_date"),
        )

    # ------------------------------------------------------------------ snapshots
    def _check_snapshots(self, snap: pl.DataFrame, latest: pl.DataFrame) -> None:
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
            "snapshot: every requisition appears in the as-of extract",
            latest,
            pl.col("snapshot_date") != pl.lit(self.as_of),
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
            pl.col("active_fills", "accepted_offer_events", "lost_after_acceptance").fill_null(0)
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

    # ------------------------------------------------------------------ applications
    def _check_applications(self, app: pl.DataFrame, stg: pl.DataFrame, latest: pl.DataFrame) -> None:
        app_req = app.join(
            latest.select("requisition_id", latest_status=pl.col("requisition_status")), on="requisition_id", how="left"
        )
        self._expect_empty(
            "application: active applications belong to open requisitions",
            app_req,
            (pl.col("application_status_current") == "active") & (pl.col("latest_status") != "open"),
        )
        self._expect_empty(
            "application: rejected_date / withdrawal_date >= application_date",
            app,
            (pl.col("rejected_date") < pl.col("application_date"))
            | (pl.col("withdrawal_date") < pl.col("application_date")),
        )
        self._expect_empty(
            "application: application_date >= requisition approval",
            app.join(latest.select("requisition_id", "approval_date"), on="requisition_id", how="left"),
            pl.col("application_date") < pl.col("approval_date"),
        )
        self._expect_empty(
            "application: rejected_date set exactly for rejected applications",
            app,
            (pl.col("application_status_current") == "rejected") != pl.col("rejected_date").is_not_null(),
        )
        self._expect_empty(
            "application: withdrawal_date set exactly for withdrawn applications",
            app,
            (pl.col("application_status_current") == "withdrawn") != pl.col("withdrawal_date").is_not_null(),
        )
        self._expect_empty(
            "application: disposition_reason only on rejected/withdrawn",
            app,
            pl.col("disposition_reason").is_not_null()
            != pl.col("application_status_current").is_in(["rejected", "withdrawn"]),
        )

        stg_app = stg.join(
            app.select("application_id", "application_status_current", "application_date", "current_stage_code"),
            on="application_id",
            how="left",
        )
        self._expect_empty("stage_history: exit >= entry", stg, pl.col("stage_exit_date") < pl.col("stage_entry_date"))
        self._expect_empty(
            "stage_history: first stage is review on the application date",
            stg_app,
            (pl.col("stage_sequence_number") == 1)
            & ((pl.col("stage_code") != "review") | (pl.col("stage_entry_date") != pl.col("application_date"))),
        )
        order = {code: i for i, code in enumerate(STAGES)}
        self._expect_empty(
            "stage_history: stages follow the governed order without skips",
            stg.with_columns(stage_index=pl.col("stage_code").replace_strict(order, return_dtype=pl.Int64)),
            pl.col("stage_index") != pl.col("stage_sequence_number") - 1,
        )
        chain = stg.sort(["application_id", "stage_sequence_number"]).with_columns(
            prev_exit=pl.col("stage_exit_date").shift(1).over("application_id")
        )
        self._expect_empty(
            "stage_history: entry of stage n+1 equals exit of stage n",
            chain,
            (pl.col("stage_sequence_number") > 1) & (pl.col("prev_exit") != pl.col("stage_entry_date")),
        )
        open_rows = stg.filter(pl.col("stage_exit_date").is_null()).group_by("application_id").len()
        self._expect_empty("stage_history: at most one open stage per application", open_rows, pl.col("len") > 1)
        open_vs_status = app.join(
            open_rows.rename({"len": "open_stages"}), on="application_id", how="left"
        ).with_columns(pl.col("open_stages").fill_null(0))
        self._expect_empty(
            "stage_history: open stage if and only if application is active",
            open_vs_status,
            (pl.col("application_status_current") == "active") != (pl.col("open_stages") == 1),
        )
        self._expect_empty(
            "stage_history: an open stage has no exit reason",
            stg,
            pl.col("stage_exit_date").is_null() & pl.col("exit_reason").is_not_null(),
        )
        # only the stage the application left from carries a reason
        last_seq = stg.group_by("application_id").agg(last_seq=pl.col("stage_sequence_number").max())
        self._expect_empty(
            "stage_history: only the final stage carries an exit reason",
            stg.join(last_seq, on="application_id", how="left"),
            pl.col("exit_reason").is_not_null() & (pl.col("stage_sequence_number") != pl.col("last_seq")),
        )
        self._expect_empty(
            "stage_history: exit reason matches the application outcome",
            stg.join(last_seq, on="application_id", how="left").join(
                app.select("application_id", "application_status_current"), on="application_id", how="left"
            ),
            (pl.col("stage_sequence_number") == pl.col("last_seq"))
            & (
                pl.col("exit_reason").is_not_null()
                != pl.col("application_status_current").is_in(STAGE_EXIT_REASONS)
            ),
        )
        last_stage = (
            stg.sort(["application_id", "stage_sequence_number"])
            .group_by("application_id", maintain_order=True)
            .agg(last=pl.col("stage_code").last())
        )
        self._expect_empty(
            "application: current_stage_code equals last stage entered",
            app.join(last_stage, on="application_id", how="left"),
            pl.col("current_stage_code") != pl.col("last"),
        )
        self._expect_empty(
            "application: offer-outcome statuses end in the offer stage",
            app,
            pl.col("application_status_current").is_in(OFFER_STAGE_STATUSES)
            & (pl.col("current_stage_code") != "offer"),
        )
        self._expect_empty(
            "stage_history: every application has a stage history",
            app.join(stg.select("application_id").unique(), on="application_id", how="anti"),
            pl.lit(True),
        )

    # ------------------------------------------------------------------ offers
    def _check_offers(self, app: pl.DataFrame, stg: pl.DataFrame, off: pl.DataFrame) -> None:
        app_off = app.join(
            off.select(
                "application_id",
                "offer_status_current",
                "offer_accepted_date",
                "offer_rescinded_date",
                "candidate_renege_date",
                has_offer=pl.lit(True),
            ),
            on="application_id",
            how="left",
        ).with_columns(pl.col("has_offer").fill_null(False))
        self._expect_empty(
            "offers: accepted-family statuses have a preserved acceptance date",
            app_off,
            pl.col("application_status_current").is_in(ACCEPTED_STATUSES) & pl.col("offer_accepted_date").is_null(),
        )
        self._expect_empty(
            "offers: non-accepted statuses have no acceptance date",
            app_off,
            ~pl.col("application_status_current").is_in(ACCEPTED_STATUSES)
            & pl.col("offer_accepted_date").is_not_null(),
        )
        self._expect_empty(
            "offers: application status agrees with the current offer status",
            app_off.filter(pl.col("has_offer")),
            (
                pl.col("application_status_current").is_in(
                    ["offer_declined", "offer_withdrawn", "offer_rescinded", "candidate_renege"]
                )
                & (pl.col("offer_status_current") != pl.col("application_status_current"))
            )
            | (
                pl.col("application_status_current").is_in(["offer_accepted", "started"])
                & (pl.col("offer_status_current") != pl.lit("accepted"))
            )
            | (pl.col("application_status_current") == "active")
            & (pl.col("offer_status_current") != pl.lit("pending")),
        )
        self._expect_empty(
            "offers: applications that reached the offer stage have an offer",
            app_off,
            (pl.col("current_stage_code") == "offer") & ~pl.col("has_offer"),
        )
        self._expect_empty(
            "offers: applications that never reached the offer stage have no offer",
            app_off,
            (pl.col("current_stage_code") != "offer") & pl.col("has_offer"),
        )
        off_app = off.join(app.select("application_id", "application_date"), on="application_id", how="left")
        self._expect_empty(
            "offer: extended >= application date", off_app, pl.col("offer_extended_date") < pl.col("application_date")
        )
        self._expect_empty(
            "offer: response dates >= extended",
            off,
            (pl.col("offer_accepted_date") < pl.col("offer_extended_date"))
            | (pl.col("offer_declined_date") < pl.col("offer_extended_date"))
            | (pl.col("offer_withdrawn_date") < pl.col("offer_extended_date")),
        )
        self._expect_empty(
            "offer: rescind / renege dates >= accepted",
            off,
            (pl.col("offer_rescinded_date") < pl.col("offer_accepted_date"))
            | (pl.col("candidate_renege_date") < pl.col("offer_accepted_date")),
        )
        self._expect_empty(
            "offer: a post-acceptance loss keeps its acceptance date",
            off,
            (pl.col("offer_rescinded_date").is_not_null() | pl.col("candidate_renege_date").is_not_null())
            & pl.col("offer_accepted_date").is_null(),
        )
        self._expect_empty(
            "offer: rescind and renege are mutually exclusive",
            off,
            pl.col("offer_rescinded_date").is_not_null() & pl.col("candidate_renege_date").is_not_null(),
        )
        self._expect_empty(
            "offer: acceptance excludes a pre-acceptance decline or withdrawal",
            off,
            pl.col("offer_accepted_date").is_not_null()
            & (pl.col("offer_declined_date").is_not_null() | pl.col("offer_withdrawn_date").is_not_null()),
        )
        self._expect_empty(
            "offer: current status agrees with the dated events",
            off,
            ((pl.col("offer_status_current") == "accepted") & pl.col("offer_accepted_date").is_null())
            | ((pl.col("offer_status_current") == "offer_declined") & pl.col("offer_declined_date").is_null())
            | ((pl.col("offer_status_current") == "offer_withdrawn") & pl.col("offer_withdrawn_date").is_null())
            | ((pl.col("offer_status_current") == "offer_rescinded") & pl.col("offer_rescinded_date").is_null())
            | ((pl.col("offer_status_current") == "candidate_renege") & pl.col("candidate_renege_date").is_null())
            | (
                (pl.col("offer_status_current") == "accepted")
                & (pl.col("offer_rescinded_date").is_not_null() | pl.col("candidate_renege_date").is_not_null())
            )
            | (
                (pl.col("offer_status_current") == "pending")
                & (
                    pl.col("offer_accepted_date").is_not_null()
                    | pl.col("offer_declined_date").is_not_null()
                    | pl.col("offer_withdrawn_date").is_not_null()
                )
            ),
        )
        self._expect_empty("offer: every offer has an extended date", off, pl.col("offer_extended_date").is_null())
        self._expect_empty(
            "offer: extended date is not before the offer stage was entered",
            off.join(
                stg.filter(pl.col("stage_code") == "offer").select("application_id", "stage_entry_date"),
                on="application_id",
                how="left",
            ),
            (pl.col("offer_extended_date") < pl.col("stage_entry_date"))
            | (pl.col("offer_accepted_date") < pl.col("stage_entry_date")),
        )
        self._expect_empty(
            "offer: planned start is not before acceptance",
            off,
            pl.col("planned_start_date") < pl.col("offer_accepted_date"),
        )

    # ------------------------------------------------------------------ HR
    def _check_hr(self, app: pl.DataFrame, hr: pl.DataFrame) -> None:
        starts = hr.filter(pl.col("event_type") == "start")
        terms = hr.filter(pl.col("event_type") == "termination")
        accepted = self._accepted_offers()
        starts_acc = starts.join(
            accepted.select("application_id", "accepted_date"), on="application_id", how="left"
        ).join(app.select("application_id", "application_status_current"), on="application_id", how="left")
        self._expect_empty(
            "worker_event: start has an accepted offer that was not lost",
            starts_acc,
            pl.col("accepted_date").is_null() | (pl.col("application_status_current") != "started"),
        )
        self._expect_empty(
            "worker_event: start date >= offer accepted date",
            starts_acc,
            pl.col("event_date") < pl.col("accepted_date"),
        )
        self._expect_empty(
            "application: started status has a start event",
            app.filter(pl.col("application_status_current") == "started").join(
                starts.select("application_id").unique(), on="application_id", how="anti"
            ),
            pl.lit(True),
        )
        self._expect_empty(
            "worker_event: only terminations carry a reason",
            hr,
            (pl.col("event_type") == "termination") != pl.col("termination_reason").is_not_null(),
        )
        self._unique(
            "worker_event: one worker per application (after de-duplication)",
            starts.select("application_id", "worker_id", "event_date").unique(),
            ["application_id"],
        )
        start_dates = starts.group_by("worker_id").agg(start=pl.col("event_date").min())
        self._expect_empty(
            "worker_event: termination >= start",
            terms.join(start_dates, on="worker_id", how="left"),
            pl.col("start").is_null() | (pl.col("event_date") < pl.col("start")),
        )
        self._expect_empty(
            "worker_event: terminated workers have a start event",
            terms.join(starts.select("worker_id").unique(), on="worker_id", how="anti"),
            pl.lit(True),
        )

    # ------------------------------------------------------------------ repeat attempts
    def _check_repeat_attempts(self, app: pl.DataFrame, stg: pl.DataFrame) -> None:
        """A candidate/requisition pair may repeat, as consecutive attempts with new IDs.

        Contract 1.3 allows a genuine second attempt at the same requisition after the first
        was lost, so pair uniqueness is the wrong rule. What must hold instead is that the
        attempts do not overlap: the earlier one has to have finished - it left the process,
        and its last stage closed - before the later one was submitted. Two live attempts by
        one person on one requisition would be an impossible person, and the active-fill
        rules below are the other half of the same guarantee.
        """
        attempt_end = stg.group_by("application_id").agg(attempt_end=pl.col("stage_exit_date").max())
        attempts = (
            app.select(
                "application_id",
                "candidate_id",
                "requisition_id",
                "application_date",
                "application_status_current",
            )
            .join(attempt_end, on="application_id", how="left")
            .sort(["candidate_id", "requisition_id", "application_date", "application_id"])
            .with_columns(
                prev_end=pl.col("attempt_end").shift(1).over("candidate_id", "requisition_id"),
                prev_status=pl.col("application_status_current").shift(1).over("candidate_id", "requisition_id"),
                prev_id=pl.col("application_id").shift(1).over("candidate_id", "requisition_id"),
            )
        )
        self._expect_empty(
            "application: a repeated attempt starts after the previous one ended",
            attempts.filter(pl.col("prev_id").is_not_null()),
            (pl.col("prev_status") == "active")
            | pl.col("prev_end").is_null()
            | (pl.col("application_date") <= pl.col("prev_end")),
        )

    # ------------------------------------------------------------------ candidate realism
    def _check_candidate_realism(self, app: pl.DataFrame, hr: pl.DataFrame) -> None:
        # A candidate may apply to several requisitions, but one person cannot hold two jobs
        # at once, nor keep interviewing elsewhere after taking a seat.
        accepted = self._accepted_offers()
        candidate_fills = (
            accepted.filter(~pl.col("is_lost"))
            .join(app.select("application_id", "candidate_id"), on="application_id", how="left")
            .select("candidate_id", "application_id")
            .unique()
        )
        self._unique("application: candidate holds at most one active fill", candidate_fills, ["candidate_id"])
        self._fail(
            "application: candidate is not still applying while holding an active fill",
            candidate_fills.select("candidate_id")
            .unique()
            .join(
                app.filter(pl.col("application_status_current") == "active").select("candidate_id").unique(),
                on="candidate_id",
                how="inner",
            ),
        )
        seat_taken = (
            accepted.filter(~pl.col("is_lost"))
            .join(app.select("application_id", "candidate_id"), on="application_id", how="left")
            .group_by("candidate_id")
            .agg(seat_taken_date=pl.col("accepted_date").min())
        )
        self._expect_empty(
            "application: candidate does not apply again after taking a seat",
            app.join(seat_taken, on="candidate_id", how="inner"),
            pl.col("application_date") > pl.col("seat_taken_date"),
        )
        self._unique(
            "worker_event: candidate has at most one start",
            hr.filter(pl.col("event_type") == "start")
            .join(app.select("application_id", "candidate_id"), on="application_id", how="left")
            .select("candidate_id", "application_id")
            .unique(),
            ["candidate_id"],
        )


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
