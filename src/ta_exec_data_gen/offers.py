"""Offer versions: the ATS offer table with realistic version history.

One offer *cycle* (offer_id) is the offer made to one application. A cycle can carry
several *versions*:

* the initial version,
* a negotiation revision before the candidate responds (version 1 becomes `superseded`),
* administrative revisions after acceptance: a moved start date, a salary correction or a
  re-issued letter. These are re-recorded as `accepted` with a later acceptance date, so a
  source application can legitimately carry more than one accepted version. dbt collapses
  them (earliest acceptance of the same offer_id wins).

A configurable handful of applications also carry a *second offer cycle* that was accepted
after the first accepted cycle was lost. These are the ambiguous cases the contract asks
dbt to quarantine and record in its audit model. Both cycles end in a loss, so no open seat
depends on how they are resolved.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from .config import GeneratorConfig
from .dates import DayIndex
from .funnel import NO_DAY
from .rng import RngFactory

LEVEL_BASE_SALARY = {1: 58_000, 2: 78_000, 3: 105_000, 4: 135_000, 5: 160_000, 6: 210_000}
JF_SALARY_FACTOR = {
    "SWE": 1.25,
    "DAT": 1.15,
    "ITS": 1.05,
    "PDM": 1.20,
    "MKT": 0.95,
    "SLS": 1.00,
    "CSM": 0.90,
    "OPS": 0.80,
    "FIN": 1.00,
    "PPL": 0.95,
}


def build_offer_versions(
    apps: pl.DataFrame, master: pl.DataFrame, cfg: GeneratorConfig, rngs: RngFactory
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (offer_versions, apps) where apps may carry updated statuses for quarantine cases.

    Day columns are integer offsets; the pipeline converts them to dates.
    """
    rng = rngs.stream("offers")
    oc = cfg.offers
    as_of = DayIndex(cfg.dates.history_start).to_day(cfg.dates.as_of)
    level_rank = {jl.code: jl.level_rank for jl in cfg.job_levels}

    base = (
        apps.filter(pl.col("offer_extended_day") != NO_DAY)
        .join(master.select("req_idx", "jf_code", "level_code"), on="req_idx", how="left")
        .sort("app_idx")
    )
    n = base.height
    ranks = np.array([level_rank[c] for c in base["level_code"].to_list()])
    jf_factor = np.array([JF_SALARY_FACTOR.get(c, 1.0) for c in base["jf_code"].to_list()])
    salary = np.array([LEVEL_BASE_SALARY[r] for r in ranks]) * jf_factor * rng.uniform(0.92, 1.10, n)
    salary = (np.round(salary / 500) * 500).astype(int)

    ext = base["offer_extended_day"].to_numpy()
    acc = base["offer_accepted_day"].to_numpy()
    dec = base["offer_declined_day"].to_numpy()
    wdr = base["offer_withdrawn_day"].to_numpy()
    res = base["offer_rescinded_day"].to_numpy()
    ren = base["candidate_renege_day"].to_numpy()
    proposed = base["proposed_start_day"].to_numpy()
    start_revised = base["start_revised"].to_numpy()
    start_day = base["start_day"].to_numpy()
    app_idx = base["app_idx"].to_numpy()
    response = np.where(acc != NO_DAY, acc, np.where(dec != NO_DAY, dec, np.where(wdr != NO_DAY, wdr, NO_DAY)))
    loss = np.where(res != NO_DAY, res, ren)

    rows: list[dict] = []

    def add(
        i: int,
        cycle: int,
        version: int,
        reason: str,
        status: str,
        extended: int,
        accepted: int = NO_DAY,
        declined: int = NO_DAY,
        withdrawn: int = NO_DAY,
        rescinded: int = NO_DAY,
        reneged: int = NO_DAY,
        proposed_start: int = NO_DAY,
        base_salary: int = 0,
    ) -> None:
        rows.append(
            {
                "app_idx": int(app_idx[i]),
                "offer_cycle_number": cycle,
                "offer_version_number": version,
                "version_reason": reason,
                "offer_status": status,
                "offer_extended_day": int(extended),
                "offer_accepted_day": int(accepted),
                "offer_declined_day": int(declined),
                "offer_withdrawn_day": int(withdrawn),
                "offer_rescinded_day": int(rescinded),
                "candidate_renege_day": int(reneged),
                "proposed_start_day": int(proposed_start),
                "base_salary": int(base_salary),
            }
        )

    u_neg = rng.random(n)
    u_admin = rng.random(n)
    for i in range(n):
        version = 1
        extended_final = int(ext[i])
        sal = int(salary[i])
        # proposed start is only meaningful once the candidate could accept; use it on every version
        prop = int(proposed[i]) if proposed[i] != NO_DAY else (int(ext[i]) + 28 if ext[i] != NO_DAY else NO_DAY)
        gap = int(response[i] - ext[i]) if response[i] != NO_DAY else (as_of - int(ext[i]))
        if u_neg[i] < oc.negotiation_revision_probability and gap >= 2:
            rev_day = int(ext[i]) + int(rng.integers(1, gap))
            add(i, 1, 1, "initial", "superseded", int(ext[i]), proposed_start=prop, base_salary=sal)
            sal = int(np.round(sal * rng.uniform(1.03, 1.08) / 500) * 500)
            version = 2
            extended_final = rev_day
        reason = "initial" if version == 1 else "negotiation_revision"
        if acc[i] != NO_DAY:
            add(
                i,
                1,
                version,
                reason,
                "accepted",
                extended_final,
                accepted=int(acc[i]),
                proposed_start=prop,
                base_salary=sal,
            )
            last_acc_day = int(acc[i])
            limit = min(
                as_of,
                int(loss[i]) if loss[i] != NO_DAY else as_of,
                int(start_day[i]) if start_day[i] != NO_DAY else as_of,
            )
            admin: list[str] = []
            if start_revised[i]:
                admin.append("start_date_revision")
            elif u_admin[i] < oc.admin_revision_probability:
                admin.append(oc.admin_revision_reasons[int(rng.integers(0, len(oc.admin_revision_reasons)))])
                if admin[0] == "start_date_revision":
                    admin[0] = "letter_reissue"
            for adm in admin:
                if last_acc_day + 2 > limit:
                    break
                day = int(rng.integers(last_acc_day + 2, min(last_acc_day + 15, limit) + 1))
                version += 1
                if adm == "start_date_revision":
                    prop = (
                        int(start_day[i])
                        if start_day[i] != NO_DAY
                        else prop
                        + int(rng.integers(oc.start_date_revision_days[0], oc.start_date_revision_days[1] + 1))
                    )
                if adm == "salary_correction":
                    sal = int(np.round(sal * rng.uniform(0.99, 1.02) / 100) * 100)
                add(i, 1, version, adm, "accepted", day, accepted=day, proposed_start=prop, base_salary=sal)
                last_acc_day = day
            # a post-acceptance loss is recorded on the current (last) version
            if loss[i] != NO_DAY:
                last = rows[-1]
                if res[i] != NO_DAY:
                    last["offer_status"] = "rescinded"
                    last["offer_rescinded_day"] = int(res[i])
                else:
                    last["offer_status"] = "reneged"
                    last["candidate_renege_day"] = int(ren[i])
        elif dec[i] != NO_DAY:
            add(
                i,
                1,
                version,
                reason,
                "declined",
                extended_final,
                declined=int(dec[i]),
                proposed_start=prop,
                base_salary=sal,
            )
        elif wdr[i] != NO_DAY:
            add(
                i,
                1,
                version,
                reason,
                "withdrawn",
                extended_final,
                withdrawn=int(wdr[i]),
                proposed_start=prop,
                base_salary=sal,
            )
        else:
            add(i, 1, version, reason, "extended", extended_final, proposed_start=prop, base_salary=sal)

    versions = pl.DataFrame(rows)

    # ------------------------------------------------------------ ambiguous second cycles
    apps_out = apps
    if oc.quarantine_case_count > 0:
        candidates = (
            base.filter((pl.col("candidate_renege_day") != NO_DAY) & (pl.col("candidate_renege_day") <= as_of - 60))
            .select("app_idx", "candidate_renege_day", "proposed_start_day")
            .sort("app_idx")
        )
        take = min(oc.quarantine_case_count, candidates.height)
        pick = np.sort(rng.choice(candidates.height, size=take, replace=False)) if take else np.zeros(0, int)
        chosen = candidates[pick.tolist()] if take else candidates.clear()
        extra_rows = []
        new_status_day: dict[int, int] = {}
        for r in chosen.iter_rows(named=True):
            first_loss = int(r["candidate_renege_day"])
            ext2 = first_loss + int(rng.integers(5, 21))
            acc2 = ext2 + int(rng.integers(1, 6))
            loss2 = min(acc2 + int(rng.integers(3, 21)), as_of)
            sal = int(np.round(LEVEL_BASE_SALARY[2] * rng.uniform(0.95, 1.1) / 500) * 500)
            extra_rows.append(
                {
                    "app_idx": int(r["app_idx"]),
                    "offer_cycle_number": 2,
                    "offer_version_number": 1,
                    "version_reason": "initial",
                    "offer_status": "reneged",
                    "offer_extended_day": ext2,
                    "offer_accepted_day": acc2,
                    "offer_declined_day": NO_DAY,
                    "offer_withdrawn_day": NO_DAY,
                    "offer_rescinded_day": NO_DAY,
                    "candidate_renege_day": loss2,
                    "proposed_start_day": acc2 + 21,
                    "base_salary": sal,
                }
            )
            new_status_day[int(r["app_idx"])] = loss2
        if extra_rows:
            versions = pl.concat([versions, pl.DataFrame(extra_rows)])
            upd = pl.DataFrame({"app_idx": list(new_status_day), "status_day_new": list(new_status_day.values())})
            apps_out = (
                apps.join(upd, on="app_idx", how="left")
                .with_columns(status_day=pl.coalesce(pl.col("status_day_new"), pl.col("status_day")))
                .drop("status_day_new")
            )

    versions = versions.sort(["app_idx", "offer_cycle_number", "offer_version_number"]).with_columns(
        is_current_version=(
            pl.col("offer_version_number") == pl.col("offer_version_number").max().over("app_idx", "offer_cycle_number")
        ),
        currency=pl.lit(oc.currency),
    )
    return versions, apps_out
