"""Candidate funnel simulation.

Each requisition is simulated as a small chronological process:

1. Candidates apply in a sourcing burst after approval (or after a seat reopens), then as
   a trickle while the requisition stays open.
2. Every application walks the governed stage flow review -> screen -> assessment ->
   interview -> offer. At each stage it spends a lognormal number of days, then either
   advances or leaves (rejected / withdrawn). At the offer stage it accepts, declines, or
   the employer withdraws the offer.
3. Seats are filled in acceptance order. The moment active fills equal the requested
   seats the requisition is full: the remaining pipeline is dispositioned a few days later
   and the posting stops taking applications.
4. An accepted offer can be lost after acceptance (candidate renege or employer rescind).
   That reopens the seat and starts a new sourcing wave. Otherwise the person starts on
   the proposed start date, which produces an HR hire event.
5. Planned cancellations, stale-requisition cancellations and partial seat cancellations
   apply only if the requisition is still open on the planned day.

Only dated events, statuses, quantities and attributes are produced. Nothing here decides
whether an acceptance is "still a fill" or whether a hire "counts": that is dbt's job.
The simulation is stateful per requisition, which is why it is written as a loop over
requisitions with numpy draws inside; everything around it is Polars.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import polars as pl

from .config import GeneratorConfig, JobFamily, JobLevel
from .dates import DayIndex, snap_to_mondays
from .rng import RngFactory

STAGES = ["review", "screen", "assessment", "interview", "offer"]
OFFER = 4
NO_DAY = -1

REJECTED_REASON_BY_STAGE = {
    0: ["not_qualified", "experience_mismatch"],
    1: ["not_qualified", "experience_mismatch"],
    2: ["assessment_failed"],
    3: ["interview_feedback"],
}


@dataclass(eq=False)
class App:
    """One application. Day values are offsets from history_start; NO_DAY means absent."""

    req_idx: int
    wave: int
    arrival: int
    entries: list[int]  # per stage index, NO_DAY when never entered
    exits: list[int]  # per stage index, NO_DAY when still open
    last_stage: int  # highest stage index entered
    status: str = "active"
    exit_reason: str | None = None  # rejected / withdrawn / offer_declined / offer_withdrawn
    disposition_reason: str | None = None
    status_day: int = NO_DAY
    offer_extended: int = NO_DAY
    offer_accepted: int = NO_DAY
    offer_declined: int = NO_DAY
    offer_withdrawn: int = NO_DAY
    offer_rescinded: int = NO_DAY
    candidate_renege: int = NO_DAY
    planned_start: int = NO_DAY
    start_revised: bool = False
    start_day: int = NO_DAY  # actual employee start (<= as_of) when it happened
    # simulation-only scratch
    natural_accept: int = NO_DAY
    natural_outcome: str = ""
    loss_day: int = NO_DAY  # renege or rescind day if it happens (<= as_of)


@dataclass(eq=False)
class ReqOutcome:
    req_idx: int
    requested_final: int
    cancelled_positions: int
    status_final: str
    cancel_day: int = NO_DAY  # day a cancellation (full or partial) was applied
    cancel_kind: str | None = None  # full / partial
    cancel_seats: int = 0  # seats removed by that cancellation
    partial_day: int = NO_DAY
    partial_seats: int = 0
    rebase1_day: int = NO_DAY
    rebase1_shift: int = 0
    rebase2_day: int = NO_DAY
    rebase2_shift: int = 0
    apps: list[App] = field(default_factory=list)


class FunnelSimulator:
    def __init__(self, cfg: GeneratorConfig, rngs: RngFactory) -> None:
        self.cfg = cfg
        self.rng = rngs.stream("funnel")
        self.idx = DayIndex(cfg.dates.history_start)
        self.as_of = self.idx.to_day(cfg.dates.as_of)
        fz = cfg.episodes.hiring_freeze
        self.freeze = (self.idx.to_day(fz.start), self.idx.to_day(fz.end))
        self.surge = cfg.episodes.hiring_surge
        self.jf = {jf.code: jf for jf in cfg.job_families}
        self.jl = {jl.code: jl for jl in cfg.job_levels}

    # ------------------------------------------------------------------ public
    def simulate_all(self, master: pl.DataFrame) -> list[ReqOutcome]:
        outcomes: list[ReqOutcome] = []
        for row in master.sort("req_idx").iter_rows(named=True):
            outcomes.append(self._simulate_requisition(row))
        return outcomes

    # ------------------------------------------------------------------ arrivals
    def _arrivals(self, start: int, end: int, seats: int, jf: JobFamily, share: float) -> np.ndarray:
        """Application arrival days between start and end (inclusive)."""
        if end < start or seats <= 0:
            return np.zeros(0, dtype=int)
        fc = self.cfg.funnel
        expected_burst = jf.apps_per_position * seats * share
        n_burst = int(self.rng.poisson(expected_burst))
        burst = start + 1 + np.floor(self.rng.gamma(1.6, fc.burst_window_days / 4.0, size=n_burst)).astype(int)
        trickle_start = start + fc.burst_window_days
        n_trickle = 0
        if end > trickle_start:
            rate_per_day = jf.apps_per_position * seats * fc.trickle_rate_share / 30.0
            n_trickle = int(self.rng.poisson(rate_per_day * (end - trickle_start)))
        trickle = self.rng.integers(trickle_start, end + 1, size=n_trickle) if n_trickle else np.zeros(0, dtype=int)
        days = np.concatenate([burst, trickle])
        days = days[days <= end]
        days.sort()
        return days[: fc.max_applications_per_requisition]

    # ------------------------------------------------------------------ paths
    def _paths(
        self, arrivals: np.ndarray, req_idx: int, wave: int, jf: JobFamily, jl: JobLevel, is_surge: bool
    ) -> list[App]:
        n = len(arrivals)
        if n == 0:
            return []
        fc = self.cfg.funnel
        dur_mult = jl.duration_multiplier * (self.surge.cycle_time_multiplier if is_surge else 1.0)
        medians = np.array(jf.stage_duration_median_days) * dur_mult
        durations = self.rng.lognormal(np.log(medians), fc.duration_sigma, size=(n, 5))
        durations = np.maximum(np.rint(durations).astype(int), 0)
        pass_p = np.array(jf.stage_pass, dtype=float)
        pass_p[3] *= jl.interview_pass_multiplier
        if is_surge:
            pass_p = pass_p * self.surge.pass_rate_multiplier
        pass_p = np.minimum(pass_p, 0.97)
        passed = self.rng.random((n, 4)) < pass_p
        accept_p = min(jf.offer_accept_rate * (self.surge.pass_rate_multiplier if is_surge else 1.0), 0.97)
        u_offer = self.rng.random(n)
        u_exit = self.rng.random(n)
        apps: list[App] = []
        for i in range(n):
            entries = [NO_DAY] * 5
            exits = [NO_DAY] * 5
            day = int(arrivals[i])
            last = 0
            outcome = ""
            for s in range(5):
                entries[s] = day
                day = day + int(durations[i, s])
                exits[s] = day
                last = s
                if s < OFFER:
                    if not passed[i, s]:
                        outcome = "withdrawn" if u_exit[i] < fc.withdrawn_share_of_exits else "rejected"
                        break
                else:
                    if u_offer[i] < accept_p:
                        outcome = "accepted"
                    elif u_offer[i] < accept_p + (1 - accept_p) * fc.offer_withdrawn_share:
                        outcome = "offer_withdrawn"
                    else:
                        outcome = "offer_declined"
            app = App(
                req_idx=req_idx, wave=wave, arrival=int(arrivals[i]), entries=entries, exits=exits, last_stage=last
            )
            app.natural_outcome = outcome
            if last == OFFER:
                app.offer_extended = entries[OFFER]
                if outcome == "accepted":
                    app.natural_accept = exits[OFFER]
            apps.append(app)
        return apps

    # ------------------------------------------------------------------ helpers
    def _finalise_natural(self, app: App) -> None:
        """Apply the application's own outcome (no cut involved)."""
        s = app.last_stage
        end = app.exits[s]
        app.status_day = end
        if app.natural_outcome == "accepted":
            app.status = "offer_accepted"
            app.offer_accepted = end
        elif app.natural_outcome == "offer_declined":
            app.status = "offer_declined"
            app.exit_reason = "offer_declined"
            app.offer_declined = end
        elif app.natural_outcome == "offer_withdrawn":
            app.status = "offer_withdrawn"
            app.exit_reason = "offer_withdrawn"
            app.offer_withdrawn = end
        elif app.natural_outcome == "withdrawn":
            app.status = "withdrawn"
            app.exit_reason = "withdrawn"
            app.disposition_reason = self._pick(self.cfg.funnel.disposition_reasons.withdrawn)
        else:
            app.status = "rejected"
            app.exit_reason = "rejected"
            app.disposition_reason = self._pick(REJECTED_REASON_BY_STAGE[s])

    def _cut(self, app: App, day: int, reason: str, full_day: int) -> None:
        """Disposition an in-process application on `day` (requisition filled or cancelled).

        No stage can be entered after `full_day` (nobody moves a candidate forward on a
        requisition that is already full), so the stage the candidate is cut from is the
        last one entered on or before that day.
        """
        stage = max(s for s in range(5) if app.entries[s] != NO_DAY and app.entries[s] <= min(day, full_day))
        for s in range(stage + 1, 5):
            app.entries[s] = NO_DAY
            app.exits[s] = NO_DAY
        app.last_stage = stage
        app.exits[stage] = day
        app.status_day = day
        app.natural_accept = NO_DAY
        if stage < OFFER:
            app.offer_extended = NO_DAY
        if stage == OFFER:
            app.status = "offer_withdrawn"
            app.exit_reason = "offer_withdrawn"
            app.offer_withdrawn = day
        elif self.rng.random() < self.cfg.funnel.cut_withdrawn_share:
            app.status = "withdrawn"
            app.exit_reason = "withdrawn"
            app.disposition_reason = self._pick(self.cfg.funnel.disposition_reasons.withdrawn)
        else:
            app.status = "rejected"
            app.exit_reason = "rejected"
            app.disposition_reason = reason

    def _truncate_as_of(self, app: App) -> None:
        """Remove anything after the as-of date; an interval straddling it stays open."""
        for s in range(5):
            if app.entries[s] != NO_DAY and app.entries[s] > self.as_of:
                app.entries[s] = NO_DAY
                app.exits[s] = NO_DAY
        stage = max(s for s in range(5) if app.entries[s] != NO_DAY)
        app.last_stage = stage
        if app.exits[stage] != NO_DAY and app.exits[stage] > self.as_of:
            app.exits[stage] = NO_DAY
        if app.exits[stage] == NO_DAY:
            app.status = "active"
            app.exit_reason = None
            app.disposition_reason = None
            app.status_day = app.entries[stage]
            for attr in (
                "offer_accepted",
                "offer_declined",
                "offer_withdrawn",
                "offer_rescinded",
                "candidate_renege",
                "planned_start",
                "start_day",
                "loss_day",
                "natural_accept",
            ):
                setattr(app, attr, NO_DAY)
            app.start_revised = False
        if app.offer_extended != NO_DAY and app.offer_extended > self.as_of:
            app.offer_extended = NO_DAY

    def _pick(self, options: list[str]) -> str:
        return options[int(self.rng.integers(0, len(options)))]

    def _post_acceptance(self, app: App, jf: JobFamily, jl: JobLevel) -> None:
        """Draw what happens after acceptance: start, renege or rescind."""
        oc = self.cfg.offers
        t = app.offer_accepted
        proposed = int(
            snap_to_mondays(np.array([t + jl.notice_days + int(self.rng.integers(-3, 11))]), self.idx.origin)[0]
        )
        proposed = max(proposed, t + 3)
        app.planned_start = proposed
        start = proposed
        if self.rng.random() < self.cfg.offers.admin_revision_probability * 0.5:
            shift = int(self.rng.integers(oc.start_date_revision_days[0], oc.start_date_revision_days[1] + 1))
            start = int(snap_to_mondays(np.array([proposed + shift]), self.idx.origin)[0])
            app.start_revised = True
        p_rescind = oc.base_rescind_probability
        if self.freeze[0] <= t <= self.freeze[1]:
            p_rescind *= self.cfg.episodes.hiring_freeze.rescind_multiplier
        u = self.rng.random()
        loss_kind = None
        if u < p_rescind:
            loss_kind = "rescind"
        elif u < p_rescind + jf.renege_rate:
            loss_kind = "renege"
        if loss_kind:
            hi = max(start - 1, t + 1)
            loss = int(self.rng.integers(t + 1, hi + 1))
            if loss <= self.as_of:
                app.loss_day = loss
                if loss_kind == "rescind":
                    app.offer_rescinded = loss
                    app.status = "offer_rescinded"
                else:
                    app.candidate_renege = loss
                    app.status = "candidate_renege"
                app.status_day = loss
                return
        if start <= self.as_of:
            app.start_day = start
            # the ATS moves the application on when the person actually starts; the start
            # itself stays an HR event (contract: event and state are separate columns)
            app.status = "started"
            app.status_day = start

    # ------------------------------------------------------------------ per requisition
    def _simulate_requisition(self, row: dict) -> ReqOutcome:
        jf = self.jf[row["jf_code"]]
        jl = self.jl[row["level_code"]]
        req_idx = int(row["req_idx"])
        approval = int(row["approval_day"])
        requested = int(row["requested_positions"])
        cancelled_positions = 0
        is_surge = bool(row["is_surge"])
        fc = self.cfg.funnel

        checkpoints = {
            "partial": int(row["partial_day"]) if row["partial_day"] != NO_DAY else None,
            "cancel": int(row["cancel_day"]) if row["cancel_day"] != NO_DAY else None,
            "stale": int(row["stale_day"]) if row["stale_day"] != NO_DAY else None,
        }
        used_checkpoints: set[str] = set()
        outcome = ReqOutcome(req_idx=req_idx, requested_final=requested, cancelled_positions=0, status_final="open")

        accepted: list[App] = []  # applications with an acceptance event (any wave)
        all_apps: list[App] = []
        wave_start = approval
        wave = 1
        full_cancelled = False

        def active_at(day: int) -> int:
            return sum(1 for a in accepted if a.offer_accepted <= day and (a.loss_day == NO_DAY or a.loss_day > day))

        while wave <= 4 and wave_start <= self.as_of:
            seats_needed = requested - active_at(wave_start)
            share = 1.0 if wave == 1 else fc.reopen_burst_share
            arrivals = self._arrivals(wave_start, self.as_of, seats_needed, jf, share)
            apps = self._paths(arrivals, req_idx, wave, jf, jl, is_surge)

            events: list[tuple[int, int, str, App | None]] = []
            for a in apps:
                if a.natural_accept != NO_DAY and a.natural_accept <= self.as_of:
                    events.append((a.natural_accept, 1, "accept", a))
            for name, day in checkpoints.items():
                if day is not None and name not in used_checkpoints and wave_start < day <= self.as_of:
                    events.append((day, 0, name, None))
            events.sort(key=lambda e: (e[0], e[1], e[3].arrival if e[3] else -1))

            full_day = None
            cancel_day = None
            for day, _, kind, app in events:
                active = active_at(day)
                if kind == "accept":
                    assert app is not None
                    if active < requested:
                        self._finalise_natural(app)
                        self._post_acceptance(app, jf, jl)
                        accepted.append(app)
                        if active + 1 == requested:
                            full_day = day
                            break
                    # a would-be acceptance on a full requisition is cut below
                elif kind == "partial":
                    used_checkpoints.add(kind)
                    seats = min(int(row["partial_seats"]), requested - active)
                    if seats > 0:
                        requested -= seats
                        cancelled_positions += seats
                        outcome.partial_day, outcome.partial_seats = day, seats
                        if active == requested:
                            full_day = day
                            break
                else:  # cancel / stale
                    used_checkpoints.add(kind)
                    if active < requested:
                        cancel_day = day
                        break

            if cancel_day is not None:
                started = [a for a in accepted if a.start_day != NO_DAY and a.start_day <= cancel_day]
                pending = [
                    a for a in accepted if a not in started and (a.loss_day == NO_DAY or a.loss_day > cancel_day)
                ]
                if started:
                    seats = requested - active_at(cancel_day)
                    requested -= seats
                    cancelled_positions += seats
                    outcome.cancel_day, outcome.cancel_kind, outcome.cancel_seats = cancel_day, "partial", seats
                else:
                    for a in pending:
                        a.loss_day = cancel_day
                        a.offer_rescinded = cancel_day
                        a.candidate_renege = NO_DAY
                        a.start_day = NO_DAY
                        a.status = "offer_rescinded"
                        a.status_day = cancel_day
                    cancelled_positions += requested
                    outcome.cancel_day, outcome.cancel_kind, outcome.cancel_seats = cancel_day, "full", requested
                    requested = 0
                    full_cancelled = True
                cut_day = cancel_day
                reason = "requisition_cancelled"
            elif full_day is not None:
                lag = int(self.rng.integers(fc.pipeline_cut_lag_days[0], fc.pipeline_cut_lag_days[1] + 1))
                cut_day = min(full_day + lag, self.as_of)
                reason = "position_filled"
            else:
                cut_day = None
                reason = ""
            closed_day = cancel_day if cancel_day is not None else full_day

            for a in apps:
                if a in accepted:
                    continue
                if cut_day is not None:
                    if a.arrival > closed_day:
                        continue  # posting closed before they applied
                    end = a.exits[a.last_stage]
                    if a.natural_outcome == "accepted":
                        self._cut(a, min(cut_day, a.natural_accept), reason, closed_day)
                    elif end > cut_day:
                        self._cut(a, cut_day, reason, closed_day)
                    else:
                        self._finalise_natural(a)
                else:
                    self._finalise_natural(a)
                all_apps.append(a)
            all_apps.extend(a for a in apps if a in accepted)

            if full_cancelled or cancel_day is not None:
                break
            if full_day is None:
                break  # still open at as-of, pipeline running
            # a loss after the fill reopens a seat and starts a new sourcing wave
            later_losses = sorted(a.loss_day for a in accepted if a.loss_day != NO_DAY and a.loss_day > full_day)
            if not later_losses:
                break
            wave_start = later_losses[0] + int(self.rng.integers(1, 11))
            wave += 1

        for a in all_apps:
            self._truncate_as_of(a)
        outcome.apps = sorted(all_apps, key=lambda a: (a.arrival, a.wave))

        # -------------------------------------------------------- final state at as-of
        active_final = active_at(self.as_of)
        outcome.requested_final = requested
        outcome.cancelled_positions = cancelled_positions
        if full_cancelled:
            outcome.status_final = "cancelled"
        elif active_final >= requested:
            outcome.status_final = "filled"
        else:
            outcome.status_final = "open"

        # -------------------------------------------------------- re-baselining of targets
        def is_open(day: int) -> bool:
            if outcome.cancel_kind == "full" and outcome.cancel_day <= day:
                return False
            req_at = int(row["requested_positions"])
            if outcome.partial_day != NO_DAY and outcome.partial_day <= day:
                req_at -= outcome.partial_seats
            if outcome.cancel_kind == "partial" and outcome.cancel_day <= day:
                req_at = active_at(outcome.cancel_day)
            return active_at(day) < req_at

        toad = int(row["toad_day"])
        if row["has_rebase"]:
            r1 = toad + int(row["rebase_delay"])
            if r1 <= self.as_of and is_open(r1):
                outcome.rebase1_day, outcome.rebase1_shift = r1, int(row["rebase_shift"])
                if row["has_rebase2"]:
                    r2 = toad + outcome.rebase1_shift + int(row["rebase_delay2"])
                    if r2 <= self.as_of and is_open(r2):
                        outcome.rebase2_day, outcome.rebase2_shift = r2, int(row["rebase_shift2"])
        return outcome


# ---------------------------------------------------------------------- frames
def outcomes_to_frames(outcomes: list[ReqOutcome]) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Flatten simulation results into (requisition_state, applications, stage_history) frames.

    Applications get a global app_idx ordered by application day. Day columns stay as
    integer offsets; conversion to dates happens in the pipeline.
    """
    req_rows = []
    app_rows = []
    for o in outcomes:
        req_rows.append(
            {
                "req_idx": o.req_idx,
                "requested_final": o.requested_final,
                "cancelled_positions": o.cancelled_positions,
                "status_final": o.status_final,
                "cancel_day": o.cancel_day,
                "cancel_kind": o.cancel_kind,
                "cancel_seats": o.cancel_seats,
                "partial_day": o.partial_day,
                "partial_seats": o.partial_seats,
                "rebase1_day": o.rebase1_day,
                "rebase1_shift": o.rebase1_shift,
                "rebase2_day": o.rebase2_day,
                "rebase2_shift": o.rebase2_shift,
            }
        )
        for a in o.apps:
            app_rows.append(
                {
                    "req_idx": a.req_idx,
                    "wave": a.wave,
                    "application_day": a.arrival,
                    "last_stage": a.last_stage,
                    "status": a.status,
                    "exit_reason": a.exit_reason,
                    "disposition_reason": a.disposition_reason,
                    "status_day": a.status_day,
                    "exit_stage": a.last_stage,
                    "offer_extended_day": a.offer_extended,
                    "offer_accepted_day": a.offer_accepted,
                    "offer_declined_day": a.offer_declined,
                    "offer_withdrawn_day": a.offer_withdrawn,
                    "offer_rescinded_day": a.offer_rescinded,
                    "candidate_renege_day": a.candidate_renege,
                    "planned_start_day": a.planned_start,
                    "start_revised": a.start_revised,
                    "start_day": a.start_day,
                    "entries": a.entries,
                    "exits": a.exits,
                }
            )
    req_state = pl.DataFrame(req_rows, schema_overrides={"cancel_kind": pl.Utf8})
    apps = pl.DataFrame(
        app_rows,
        schema_overrides={
            "exit_reason": pl.Utf8,
            "disposition_reason": pl.Utf8,
            "entries": pl.List(pl.Int64),
            "exits": pl.List(pl.Int64),
        },
    )
    apps = (
        apps.sort(["application_day", "req_idx", "wave"], maintain_order=True)
        .with_row_index("app_idx")
        .with_columns(pl.col("app_idx").cast(pl.Int64))
    )
    stage = (
        apps.select("app_idx", "entries", "exits", "exit_stage", "exit_reason")
        .with_columns(stage_index=pl.int_ranges(0, 5))
        .explode(["stage_index", "entries", "exits"], empty_as_null=True)
        .filter(pl.col("entries") != NO_DAY)
        .rename({"entries": "stage_entry_day", "exits": "stage_exit_day"})
        .with_columns(
            stage_code=pl.col("stage_index").replace_strict(dict(enumerate(STAGES)), return_dtype=pl.Utf8),
            stage_sequence=pl.col("stage_index") + 1,
            # only the stage the application left the process from carries a reason;
            # advancing to the next stage, and an offer stage closed by an acceptance,
            # are successful exits and stay null
            exit_reason=pl.when(pl.col("stage_index") == pl.col("exit_stage"))
            .then(pl.col("exit_reason"))
            .otherwise(pl.lit(None, dtype=pl.Utf8)),
        )
        .drop("exit_stage")
        .sort(["app_idx", "stage_sequence"])
    )
    apps = apps.drop(["entries", "exits", "exit_stage"])
    return req_state, apps, stage
