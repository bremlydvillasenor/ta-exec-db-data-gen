# Design notes, assumptions and contract observations

## Boundary

`ta-exec-db` states the rule this repository follows: **Python may invent a record; it may
not decide what a record means.** The generator produces dated events, statuses,
quantities and attributes for five ATS / HR extracts plus three organisation reference
tables. Every governed derivation (`is_active_fill`, `days_to_toad`, cohort maturity, stage
conversion, yields, marts) is left to dbt, and the test suite fails if a column name that
looks like one of those derivations appears in an output.

The dbt project shape in `dbt-ownership.md` section 7 names five staging models; the file
names here map one-to-one onto them (`ats_requisition_snapshot`, `ats_application`,
`ats_offer_version`, `ats_stage_history`, `hr_worker_event`).

## Why a per-requisition simulation

Seat filling is stateful: which candidate fills a seat depends on who accepted first, a
renege reopens the seat, a cancellation only matters if the requisition is still open. That
logic is written as a small chronological loop per requisition (`funnel.py`) with numpy
draws inside. Everything before it (demand plan, segments, lifecycle plans) and after it
(candidate pool, offer versions, HR events, snapshots, validation, summary) is vectorised
Polars. About 3,000 requisitions simulate in under 3 seconds.

Determinism: one seed feeds named `SeedSequence` streams (`requisitions`, `funnel`,
`candidates`, `offers`, `hr`), so a change in one module does not reshuffle another, and
requisitions are processed in a fixed order.

## How the story is encoded (all in `config/default.yaml`)

| Story element | Mechanism |
|---|---|
| Demand variation | monthly base x growth x seasonality, surge multiplier May-Aug 2025; approval lead time decides which future requisitions already exist |
| Fill Rate differences | job family `apps_per_position`, stage pass rates and offer acceptance; multi-seat requisitions in Sales / CS / Operations |
| TOAD risk | TOAD = THD minus a level-dependent lead; missed TOADs on slow segments; re-baselining pushes some open requisitions into future months where the High / Medium / On Track bands live |
| Constraints | chosen per snapshot from evidence (thin pipeline -> qualified candidates / niche skills; declines or reneges -> compensation / candidate availability; candidates stuck late -> hiring manager delay / interview capacity; not past TOAD -> mostly no material constraint) |
| Pipeline strength | arrival bursts and trickles per seat, cut when the requisition fills; what is still open on the as-of date is the active pipeline |
| Stage conversion and duration | per-family pass rates and lognormal durations, level multipliers, surge multiplier |
| Offer outcomes | accept / decline / employer withdrawal at the offer stage; post-acceptance renege (family rate) and rescind (base rate x freeze multiplier, plus cancellation-driven rescinds) |
| Forecast potential | a real active pipeline with different depth by segment, on open requisitions only, so stage-to-active-fill yields trained on history differ by segment |
| 60-day early attrition | family rate x level multiplier x surge multiplier on started hires; later exits from a flat annual hazard |
| Future demand without future events | paths are truncated at the as-of date; planned dates (THD, TOAD, proposed start) may be later |

## Assumptions and interpretations of the contract

1. **TOAD is never after THD.** The contract defines TOAD as the date an offer must be
   accepted "for the position to remain on track for the Target Hire Date", so the
   generator keeps `approval_date <= TOAD <= THD` on every snapshot. A consequence worth
   stating for the dashboard: **every open seat whose THD is on or before the as-of date is
   in the Missed band**, and the High Risk, Medium Risk and On Track bands only contain
   seats with a THD after 31 May 2026. The wireframe's illustrative figures show all four
   bands under a "Jan-May 2026" THD selection, which is not reproducible under this
   invariant; the risk visuals need a THD range that extends past the as-of date (or no THD
   filter) to show the mix. This is the one place where the wireframe and the metric
   definitions pull in different directions; the generator follows the definitions. See
   open question 1 below for the options this leaves the contract.
2. **The ATS has no `started` or `hired` status.** `application_status` stops at
   `offer_accepted`; the actual start is an HR event. dbt derives `started` by joining
   `hr_worker_event`, exactly as the contract's event-over-status rule intends.
3. **Cancelled requisitions show `requested_positions = 0`** and their seats in
   `cancelled_positions`, following "net active demand, cancelled seats already removed".
   A partial cancellation reduces `requested_positions` and increases `cancelled_positions`,
   so `requested + cancelled` is constant per requisition and the seat identity still holds.
4. **Full cancellation rescinds pending accepted offers** (employer rescind on the
   cancellation date). If someone has already started, only the open seats are cancelled
   and the requisition becomes `filled`.
5. **Requisition snapshots are monthly** (every month-end) and stop 120 days after closure.
   The latest snapshot on or before the as-of date is the reporting state; for
   requisitions closed long ago that snapshot is older than 31 May 2026 but nothing changed
   afterwards. 31 May 2026 is itself a month-end, so every requisition open or recently
   closed has an as-of snapshot.
6. **Offer resolution key.** Accepted versions sharing an `offer_id` are administrative
   revisions (collapse, earliest date). Accepted versions on different `offer_id`s of one
   application are the ambiguous case to quarantine. The default run creates 4 of them and
   both cycles are lost, so quarantining them cannot break the requisition seat identity.
7. **Re-baselining moves THD and TOAD together.** Re-baselined requisitions therefore move
   between THD months across snapshots; the resolved snapshot decides the month.
8. **Stage flow is strictly linear with no returns and no skips**, which keeps the
   contract's uniqueness rule on (application, stage, entry date) trivially true.
9. **Reference tables have no `Unknown` member.** The dimension contracts ask dbt to add key
   -1; a source extract would not carry it.
10. **Candidates are a degenerate key, but still one person.** No candidate table is
    produced (the contract keeps candidates out of scope); candidate ids are reused across
    requisitions to make the (candidate, requisition) uniqueness rule meaningful. Reuse is
    constrained so the source never describes an impossible person: no candidate holds two
    live acceptances, none is hired twice, and none is still an active candidate elsewhere
    after taking a seat. A merge that would break one of those is undone and the
    application keeps its own candidate, which is why the realised reuse rate is a little
    below `funnel.candidate_pool_reuse`. Nothing in the fact grain requires this - it keeps
    hire counts and attrition analysis at source level believable.
11. **HR duplicates are deliberate.** About 2% of hire events are exact re-sends and about
    3% of terminations have a later-dated second row, so dbt's "one row per started hire,
    earliest termination after start" rule has something to do.
12. The `hiring_constraints` list and stage codes in the configuration mirror the contract
    seed rows; the tests assert the vocabulary. Labels, keys, SLA days and risk bands stay
    in the dbt seeds.
13. **Planned demand reaches the contract's THD ceiling, thinly.** A requisition only exists
    if it was approved on or before the as-of date, so a Target Hire Date of 31 May 2027
    requires an approval lead of a full year. `demand.early_plan_lead_days` allows up to 450
    days for the annual-plan share, which is why demand runs to the ceiling instead of
    fading out in early 2027; volumes taper from about 150 positions in June 2026 to a
    handful a month in 2027, which is what a real plan looks like that far out.

## Validation split

`ta-gen validate` (82 checks) proves the source is internally consistent: keys, foreign
keys, vocabularies, no actual date after the as-of date, target dates inside the horizon,
TOAD between approval and THD, seat quantities and status consistency on every snapshot,
the identity `requested = active fills + openings` on the latest snapshot derived from the
offer events, one open stage exactly for active applications, stage chaining, offer status
versus dates, HR starts only for accepted offers that were not lost, and the three
candidate-realism rules in assumption 10. The contract's analytics rules (section 12 of
`spec.md`) are dbt tests and are not duplicated here.

`uv run pytest` runs the generator on scaled-down configurations and checks determinism,
validation, contract alignment (columns, vocabularies, boundaries, no derived columns) and
the story itself. The story tests cover segment differences, populated risk bands, the
constraint-risk link, the interview bottleneck, offer losses, reopened requisitions and
surge cohorts leaving earlier without a perfect relationship; `test_forecast.py` adds the
parts a configuration change could weaken silently: quarantine containment, the Time to
Fill population, the stage-yield fallback order and floor, per-segment yield variation, the
requisition-level cap actually binding, forecast lift and reconciliation between month and
summary grain, the risk-band behaviour under each THD selection, and planned demand
reaching the future THD ceiling.


## Open questions for the contract (`ta-exec-db`)

These came out of building the data. None of them is a defect in the generated records, and
none is fixed here: they are decisions for the contract repository, listed so they are not
lost.

1. **The risk visual's default THD selection cannot show a band mix.** EXEC-07 uses the
   as-of date for the band and THD for period selection, and TOAD is on or before THD by
   definition. A THD window that ends on the as-of date therefore contains only Missed
   positions - 102 of 102 in the current data - while the wireframe draws 22 / 19 / 20 /
   112 under exactly that window. Retuning volumes does not help; the ratio stays 100%
   Missed. Three ways out, in order of preference:
   a. default the THD slicer to a window that reaches past the as-of date (for example the
      next 12 THD months), which is also the more natural executive question - "what is at
      risk in the demand still ahead of us";
   b. exempt the risk visuals from `thd_period`, the way the quality visuals already are,
      and say so in the filter-behaviour note;
   c. keep the current default and relabel the visual as an overdue-demand breakdown, since
      that is all it can show.
   `risk_band_by_thd_window` in `ta-gen summary` prints the mix under each option.
2. **Time to Fill has two different populations in the contract.** `spec.md` (EXEC-05) says
   "every accepted-offer event, on or before the as-of date"; `metric-def.yaml` says "every
   accepted-offer event ... on non-cancelled requisitions with THD in the selected period".
   The two differ by 33 acceptances in the current data (median 40 days either way, but the
   count is what reconciles with SUPP-06). This summary follows `metric-def.yaml` because it
   is the machine-readable definition the marts are built from. One of the two documents
   needs to change.
3. **The wireframe's figures are illustrative.** They predate this generator and are not
   produced by any seed of this configuration; section 10 of `data_story.md` reconciles
   them. Replace them with generated numbers before the wireframe is used as an acceptance
   reference, otherwise every build will look like it is failing.
4. **The `ta-exec-db` README still describes one implementation repository** owning both the
   Python generation and the dbt transformations. The working split is three repositories:
   `ta-exec-db` (specification and contracts), this one (Python raw-data generation) and a
   separate dbt project. `dbt-ownership.md` already reads correctly; the README does not.
