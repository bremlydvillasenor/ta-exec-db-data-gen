# Design notes, assumptions and contract observations

Implements `ta-exec-db` **contract release 1.3**, commit `87f8cf2`.

## Boundary

`ta-exec-db` states the rule this repository follows: **Python may invent a record; it may
not decide what a record means.** The generator produces dated events, statuses,
quantities and attributes for five ATS / HR extracts plus three organisation reference
tables. Every governed derivation (`is_active_fill`, `days_to_toad`, cohort maturity, stage
conversion, yields, marts) is left to dbt, and the test suite fails if a column name that
looks like one of those derivations appears in an output.

`dbt-ownership.md` names five staging models; the file names here map one-to-one onto them
(`ats_requisition_snapshot`, `ats_application`, `ats_offer`, `ats_stage_history`,
`hr_worker_event`). The contract's logical file names and this repository's source-prefixed
names are mapped in `data_dictionary.md`, which is the option the contract allows an
existing generator.

## Why a per-requisition simulation

Seat filling is stateful: which candidate fills a seat depends on who accepted first, a
renege reopens the seat, a cancellation only matters if the requisition is still open. That
logic is written as a small chronological loop per requisition (`funnel.py`) with numpy
draws inside. Everything before it (demand plan, segments, lifecycle plans) and after it
(candidate pool, offer versions, HR events, snapshots, validation, summary) is vectorised
Polars. About 3,000 requisitions simulate in under 3 seconds.

Determinism: one seed feeds named `SeedSequence` streams (`requisitions`, `funnel`,
`candidates`, `offers`, `hr`), so a change in one module does not reshuffle another, and
requisitions are processed in a fixed order. Raw timestamps sit outside that scheme on
purpose: `extracted_at` is configured, and the clock time inside an `updated_at` day comes
from a stable CRC of the record key, so it never depends on row order or on how many draws
another module made.

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
| Source edits | a re-negotiated letter before the response, and a moved start date, corrected salary or re-issued letter after acceptance, each editing the one offer row and advancing its `updated_at` |
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
2. **`application_status_current` includes `started`, but the start itself is still an HR
   event.** Contract 1.3 lists `started` in the application status vocabulary, so the ATS
   moves the application on when the person actually starts. The status stays *mutable
   state*: the dated start event lives in `hr_worker_event`, and dbt must derive
   `is_started` from that event, never from this status text. The removed value `hired`
   does not appear anywhere.
3. **Cancelled requisitions show `requested_positions = 0`** and their seats in
   `cancelled_positions`, following "net active demand, cancelled seats already removed".
   A partial cancellation reduces `requested_positions` and increases `cancelled_positions`,
   so `requested + cancelled` is constant per requisition and the seat identity still holds.
4. **Full cancellation rescinds pending accepted offers** (employer rescind on the
   cancellation date). If someone has already started, only the open seats are cancelled
   and the requisition becomes `filled`.
5. **Requisition snapshots are monthly, and the as-of extract is complete.** The contract
   asks for at least one snapshot per requisition on the as-of date plus earlier snapshots
   for a subset. Every requisition therefore appears on 31 May 2026, including ones
   cancelled or filled in 2024; earlier month-end rows are retained while the requisition
   was open and for 120 days after it closed. A long-closed requisition reappears in the
   as-of extract carrying the `updated_at` it had when it last really changed, which is the
   contract's rule that re-exporting an unchanged row must not restamp it.
6. **One current offer per application, no version model.** Contract 1.3 replaced offer
   versions with a current-state extract, so `ats_offer` has `application_id` as its key
   and no cycle or version identifier exists. A revision - a re-negotiated letter, a moved
   start date, a corrected salary - edits that row and advances `updated_at`. There is no
   ambiguous multi-acceptance case left to quarantine and no dbt audit model to feed:
   a duplicate key inside one extract is now a validation failure, not a case to resolve.
   Rescinded and reneged offers stay in the extract with their acceptance date preserved,
   so a missing row can never be read as a loss.
7. **Re-baselining moves THD and TOAD together.** Re-baselined requisitions therefore move
   between THD months across snapshots; the resolved snapshot decides the month.
8. **Stage flow is strictly linear with no returns and no skips**, which keeps the
   contract's uniqueness rule on (application, stage, entry date) trivially true.
9. **Reference tables have no `Unknown` member.** The dimension contracts ask dbt to add key
   -1; a source extract would not carry it.
10. **Candidates are a degenerate key, but still one person.** No candidate table is
    produced (the contract keeps candidates out of scope); candidate ids are reused so the
    source describes real people rather than one person per application. There are two kinds
    of reuse:
    * **across requisitions** (`funnel.candidate_pool_reuse`): 3,733 candidates applied to
      more than one requisition;
    * **to the same requisition again after a loss** (`funnel.reapplication_share`): 1,695
      second attempts, each with a **new** application id. Contract 1.3 allows the
      candidate/requisition pair to repeat, so this is a valid pattern, not a duplicate.
      1,384 follow a rejection, 307 a withdrawal, 3 a declined offer and 1 a candidate
      renege - and 68 of the second attempts went on to succeed, which is what a real
      "we came back for them" story looks like.

    Reuse is constrained so the source never describes an impossible person: no candidate
    holds two live acceptances, none is started twice, none is still an active candidate
    elsewhere after taking a seat, none submits a new application after the day they accepted
    one, and two attempts at the same requisition never overlap - the earlier one must have
    left the process and closed its last stage before the later one is submitted. That last
    pair of rules needs the application date rather than the final status: an application in
    flight before an acceptance and later rejected is fine, one *submitted* afterwards is
    not. A merge that would break any of them is undone and the application keeps its own
    candidate, which is why the realised reuse rate is a little below the configured share.
11. **HR duplicates are deliberate.** About 2% of start events are exact re-sends and about
    3% of terminations have a later-dated second row, so dbt's "one row per started hire,
    earliest termination after start" rule has something to do. They carry distinct
    `worker_event_id`s, so the key is still unique inside the extract.
14. **`updated_at` is derived from real source changes, not invented.** Each file has a
    documented change day (see `data_dictionary.md`); the clock time inside that day comes
    from a stable hash of the record key. Two consequences are deliberate and testable: an
    unchanged requisition row repeated in a later monthly extract keeps its timestamp
    (13,519 such repeats in the default run), and a real change advances it, never
    backwards. `timestamps.updated_at_available` records whether a source supplies a
    reliable change timestamp at all; a real export without one sets it to `false` and is
    reloaded by full comparison.
15. **Extraction time is configured, not read from the clock.** `extracted_at` defaults to
    `2026-05-31T23:59:59Z` - the end of the as-of day - and is written into
    `manifest.json`, so the same seed and configuration reproduce byte-identical files. A
    later export time is allowed by the contract and would not make any business event
    eligible; the business cutoff stays the as-of day.
12. The `hiring_constraints` list and stage codes in the configuration mirror the contract
    seed rows; the tests assert the vocabulary. Labels, keys, SLA days and risk bands stay
    in the dbt seeds.
16. **An application whose offer is still `pending` is `active`.** The application status
    vocabulary has no "offer extended" value, so an application waiting for a response sits
    at `active` with an open Offer stage, while its offer row carries
    `offer_status_current = pending`. 18 applications are in that state on the as-of date.
17. **The inactivity policy is structural, not a cut-off rule.** Contract 1.3 dropped
    `last_updated_date`, and `updated_at` is change metadata that no business rule may read.
    An application therefore stays `active` only while it is genuinely in an open stage on
    an open requisition: when a requisition fills or is cancelled, its remaining pipeline is
    dispositioned within `funnel.pipeline_cut_lag_days` with a dated reason. The result is
    that no stale application survives - on the as-of date the oldest active application was
    submitted on 22 March 2026 (70 days), and the oldest open stage is 57 days old. dbt
    should measure staleness from the open stage's entry date in `ats_stage_history`, never
    from `updated_at` and never from the application year.
13. **Planned demand reaches the contract's THD ceiling, thinly.** A requisition only exists
    if it was approved on or before the as-of date, so a Target Hire Date of 31 May 2027
    requires an approval lead of a full year. `demand.early_plan_lead_days` allows up to 450
    days for the annual-plan share, which is why demand runs to the ceiling instead of
    fading out in early 2027; volumes taper from about 150 positions in June 2026 to a
    handful a month in 2027, which is what a real plan looks like that far out.

## Validation split

`ta-gen validate` (160 checks) proves the source is internally consistent: unique keys
inside the extract, foreign keys, vocabularies, no actual date after the as-of date, target
dates inside the horizon, the raw timestamp rules, TOAD between approval and THD, seat
quantities and status consistency on every snapshot, the identity
`requested = active fills + openings` on the resolved snapshot derived from the offer
events, one open stage exactly for active applications, stage chaining, exit reasons only
on the stage an application actually left from, current offer status against the dated
events, HR starts only for accepted offers that were not lost, non-overlapping repeat
attempts, and the candidate-realism rules in assumption 10. The contract's analytics rules
(section 12 of `spec.md`) are dbt tests and are not duplicated here.

Before any of that, the validator checks the **declared shape** of every file: the columns
that must be there, their data type, and which of them may never be empty. That check is a
precondition and it stops the run when it fails, because every rule after it assumes the
shape holds. It is not a formality - a comparison against null evaluates to null, so a
required identifier, status, date or quantity that arrives empty would otherwise slip
through every range and consistency rule downstream without a single failure.

The timestamp checks are the group added for contract 1.3: both columns present on every
file including the lookups, one `extracted_at` for the whole batch, `updated_at <=
extracted_at`, every change known by the synthetic business cutoff, no requisition snapshot
claiming a change later than its own snapshot date, and `updated_at` never moving backwards
for one requisition across extracts. Each row's `updated_at` must also be on or after the
**latest** date recorded on it - the stage exit, not just the entry; the acceptance or loss,
not just the extension. A timestamp frozen at a record's opening event is the dangerous
case, because an incremental load reading watermarks would never pick the change up.

`data/fixtures/invalid/` holds the deliberately invalid extracts the contract asks to be
kept separate: twelve complete but broken batches, one per documented violation - a
duplicate offer key, a renege that erases its acceptance, an HR event after the as-of date,
`updated_at` after `extracted_at`, a broken seat identity, a missing identifier, status,
date, quantity and lookup label, a stage exit recorded without advancing `updated_at`, and
a second attempt submitted before the first one ended. `ta-gen fixtures` regenerates them,
and `test_validation.py` asserts that each one trips the check it is meant to trip - a
fixture that failed for some unrelated reason would not prove anything. The suite also
nulls **every** required column of every file in turn and asserts each one is caught.

`uv run pytest` runs the generator on scaled-down configurations and checks determinism,
validation, contract alignment (columns, vocabularies, boundaries, no derived columns) and
the story itself. `test_contract.py` asserts every contract-1.3 minimum column is present,
that no offer version or cycle identifier exists, that an unchanged requisition row repeats
its `updated_at` while a real change advances it, and that a candidate can reapply to the
same requisition after a loss without the two attempts overlapping. The story tests cover segment
differences, populated risk bands, the constraint-risk link, the interview bottleneck,
offer losses, reopened requisitions and surge cohorts leaving earlier without a perfect
relationship; `test_forecast.py` adds the parts a configuration change could weaken
silently: the one-acceptance-per-application scope limit, lost offers staying in the
extract with their acceptance preserved, the Time to Fill population, the stage-yield
fallback order and floor, per-segment yield variation, the requisition-level cap actually
binding, forecast lift and reconciliation between month and summary grain, the risk-band
behaviour under each THD selection, and planned demand reaching the future THD ceiling.

One test needs the assertion to be about counts rather than nulls, because the failure it
guards against is silent. An active candidate whose segment and stage have no trained yield
would forecast as zero, so `stage_yields` covers every combination the live pipeline
contains and `expected_pipeline_fills` raises rather than substituting a zero.


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
2. ~~**Time to Fill has two different populations in the contract.**~~ **Not a conflict -
   corrected here.** An earlier version of these notes read `spec.md` (EXEC-05) and
   `metric-def.yaml` as contradicting each other, and justified following the YAML because
   it is machine-readable. That justification was wrong: the `ta-exec-db` README says
   plainly that **file format does not determine authority**, and `spec.md` outranks
   `metric-def.yaml` in any case.

   The two statements describe different things. `spec.md` gives the **event population**:
   every accepted-offer event on or before the as-of date, including offers later rescinded
   or reneged, because Time to Fill measures the recruiting cycle TA actually completed.
   `spec.md`'s own "Reporting eligibility and historical attribution" section then adds the
   **reporting rule**: derive `is_delivery_eligible = NOT is_cancelled` from the resolved
   snapshot, exclude ineligible rows from delivery metrics, and keep those rows and their
   events in the facts for audit. `metric-def.yaml` states the same metric after that filter
   and after the THD period selection.

   So the events are preserved and the reported figure is filtered - two layers, not two
   definitions. In the current data that is 3,721 acceptance events in the source, of which
   33 sit on requisitions later cancelled; the reported EXEC-05 population is the remaining
   3,688. `time_to_fill_population` in `ta-gen summary` prints both ends so the difference
   is visible rather than surprising, and this generator preserves every acceptance event
   either way - which side of the filter a row falls on is dbt's decision, not the source's.
3. **The wireframe's figures are illustrative.** They predate this generator and are not
   produced by any seed of this configuration; section 10 of `data_story.md` reconciles
   them. Replace them with generated numbers before the wireframe is used as an acceptance
   reference, otherwise every build will look like it is failing.
4. ~~**The `ta-exec-db` README still describes one implementation repository.**~~
   **Resolved in contract 1.3.** The README's "Repository responsibilities" table and
   `spec.md` section 13 now name the three-repository split explicitly.
5. **`offers.csv` has no "started" signal, by design - confirm the join.** The contract
   keeps `accepted` as the offer status for both a pending start and someone who already
   started, and puts the actual start in `worker_events.csv`. That is correct, and it means
   `fct_hire_outcome` can only be built by joining the HR start event to the application;
   there is no shortcut through the offer status. This repository generates it that way
   (190 accepted offers waiting for a start date on the as-of date against 3,290 started),
   but it is worth stating so the dbt build does not read `accepted` as "hired".
6. **The optional incremental demonstration is not built here.** The contract offers, as an
   option, two dated full extracts showing an unchanged offer, a changed planned start, a
   renege that preserves acceptance and a new offer. The timestamp semantics those batches
   would demonstrate are already generated and tested inside one batch (see assumption 14),
   so the second dated batch was left out rather than half-built. If the portfolio wants the
   two-batch demonstration, it is a small addition on top of the current `extracted_at`
   configuration - say so and it will be added.
