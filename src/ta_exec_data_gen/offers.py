"""The current offer extract: one row per application that received an offer.

Contract 1.3 replaced the offer-version history with a **current-state** extract. An
application that reached the Offer stage has exactly one row here, carrying the offer as
it stands on the extraction date:

* `offer_status_current` — `pending`, `accepted`, or one of the four reserved loss terms
  (`offer_declined`, `offer_withdrawn` before acceptance; `offer_rescinded`,
  `candidate_renege` after it);
* every dated event that actually happened, with `offer_accepted_date` **preserved** when
  the offer was later rescinded or reneged;
* `planned_start_date`, the start date agreed in the letter.

Revisions still happen — a re-negotiated salary before the response, a moved start date or
a corrected salary after acceptance — but they now **edit this row and advance
`updated_at`**, which is exactly the behaviour the raw-timestamp rules ask a source system
to expose. No offer cycle or version identifier is produced, and one application can never
carry more than one acceptance.
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

OFFER_STATUSES = ["pending", "accepted", "offer_declined", "offer_withdrawn", "offer_rescinded", "candidate_renege"]


def build_offers(apps: pl.DataFrame, master: pl.DataFrame, cfg: GeneratorConfig, rngs: RngFactory) -> pl.DataFrame:
    """Return the current offer per application, with the day the record last changed.

    Day columns are integer offsets; the pipeline converts them to dates and timestamps.
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
    proposed = base["planned_start_day"].to_numpy()
    start_revised = base["start_revised"].to_numpy()
    start_day = base["start_day"].to_numpy()
    app_idx = base["app_idx"].to_numpy()
    response = np.where(acc != NO_DAY, acc, np.where(dec != NO_DAY, dec, np.where(wdr != NO_DAY, wdr, NO_DAY)))
    loss = np.where(res != NO_DAY, res, ren)

    u_neg = rng.random(n)
    u_admin = rng.random(n)
    rows: list[dict] = []
    for i in range(n):
        sal = int(salary[i])
        extended = int(ext[i])
        # planned start only becomes meaningful once the candidate could accept, but the
        # letter always names one, so a pending offer carries a provisional date too
        planned = int(proposed[i]) if proposed[i] != NO_DAY else extended + 28
        changed = extended

        # a re-negotiated letter before the candidate responds: same offer, new terms
        gap = int(response[i] - ext[i]) if response[i] != NO_DAY else (as_of - extended)
        if u_neg[i] < oc.negotiation_revision_probability and gap >= 2:
            extended = extended + int(rng.integers(1, gap))
            sal = int(np.round(sal * rng.uniform(1.03, 1.08) / 500) * 500)
            changed = extended

        if acc[i] != NO_DAY:
            status = "accepted"
            changed = int(acc[i])
            # administrative edits after acceptance, applied to this same row
            limit = min(
                as_of,
                int(loss[i]) if loss[i] != NO_DAY else as_of,
                int(start_day[i]) if start_day[i] != NO_DAY else as_of,
            )
            edit = None
            if start_revised[i]:
                edit = "start_date_revision"
            elif u_admin[i] < oc.admin_revision_probability:
                edit = oc.admin_revision_reasons[int(rng.integers(0, len(oc.admin_revision_reasons)))]
                if edit == "start_date_revision":
                    edit = "letter_reissue"
            if edit is not None and changed + 2 <= limit:
                day = int(rng.integers(changed + 2, min(changed + 15, limit) + 1))
                if edit == "start_date_revision":
                    planned = (
                        int(start_day[i])
                        if start_day[i] != NO_DAY
                        else planned
                        + int(rng.integers(oc.start_date_revision_days[0], oc.start_date_revision_days[1] + 1))
                    )
                elif edit == "salary_correction":
                    sal = int(np.round(sal * rng.uniform(0.99, 1.02) / 100) * 100)
                changed = day
            # a post-acceptance loss changes the status on this row; acceptance is preserved
            if res[i] != NO_DAY:
                status = "offer_rescinded"
                changed = max(changed, int(res[i]))
            elif ren[i] != NO_DAY:
                status = "candidate_renege"
                changed = max(changed, int(ren[i]))
        elif dec[i] != NO_DAY:
            status = "offer_declined"
            changed = int(dec[i])
        elif wdr[i] != NO_DAY:
            status = "offer_withdrawn"
            changed = int(wdr[i])
        else:
            status = "pending"

        rows.append(
            {
                "app_idx": int(app_idx[i]),
                "offer_status_current": status,
                "offer_extended_day": extended,
                "offer_accepted_day": int(acc[i]),
                "offer_declined_day": int(dec[i]),
                "offer_withdrawn_day": int(wdr[i]),
                "offer_rescinded_day": int(res[i]),
                "candidate_renege_day": int(ren[i]),
                "planned_start_day": planned,
                "base_salary": sal,
                "offer_changed_day": min(changed, as_of),
            }
        )

    return pl.DataFrame(rows).sort("app_idx").with_columns(currency=pl.lit(oc.currency))
