# Source data dictionary

Raw ATS and HR extracts generated for the TA Executive Dashboard, aligned with
`ta-exec-db` **contract release 1.3** (commit `87f8cf2`). Every table here is a **source**
table: dbt derives the governed flags, keys and measures from these dates, statuses and
quantities. Dates are ISO `YYYY-MM-DD`, timestamps are UTC ISO 8601 (`2026-05-31T23:59:59Z`),
booleans are `true` / `false`, nulls are empty strings.

## File names: contract name and generated name

The contract lists logical file names and allows an existing generator to keep equivalent
names with a documented mapping. This repository keeps the source-system prefix, because
the staging layer is split by source system (`stg_ats__*`, `stg_hr__*`). The mapping is
one-to-one:

| Contract file | Generated file | dbt staging model |
|---|---|---|
| `requisition_snapshots.csv` | `ats_requisition_snapshot.csv` | `stg_ats__requisition_snapshot` |
| `applications.csv` | `ats_application.csv` | `stg_ats__application` |
| `offers.csv` | `ats_offer.csv` | `stg_ats__offer` |
| `stage_history.csv` | `ats_stage_history.csv` | `stg_ats__stage_history` |
| `worker_events.csv` | `hr_worker_event.csv` | `stg_hr__worker_event` |
| `business_units.csv` | `ats_business_unit.csv` | source for `dim_business_unit` |
| `job_families.csv` | `ats_job_family.csv` | source for `dim_job_family` |
| `job_levels.csv` | `ats_job_level.csv` | source for `dim_job_level` |

Column names follow the contract exactly. Columns beyond the contract minimum are marked
*source realism* below; they describe the source system and no metric depends on them.

## `updated_at` and `extracted_at` (every file, every row)

These two columns are **change and extraction metadata, not business dates**. No metric
reads them.

| Column | Meaning |
|---|---|
| `updated_at` | When this source record was last modified — including a status or date correction. It advances only on a real source change; re-exporting an unchanged row leaves it where it was, and it never moves backwards. |
| `extracted_at` | When the complete extract was produced. Identical on every row of every file in one batch, configured (`timestamps.extracted_at`, default `2026-05-31T23:59:59Z`) rather than read from the clock, and recorded in `manifest.json`. |

`updated_at <= extracted_at` always. Every generated change is known by the synthetic
business cutoff (end of the as-of day), and an earlier requisition snapshot never carries a
change later than its own `snapshot_date`. What advances `updated_at` per file:

| File | The record changed when… |
|---|---|
| `ats_requisition_snapshot` | it was approved, re-baselined, partially or fully cancelled, a seat was filled or reopened, or the recorded constraint changed |
| `ats_application` | it was submitted, its stage moved, or its status changed (disposition, acceptance, post-acceptance loss, actual start) |
| `ats_stage_history` | the stage was entered, then again when its exit was recorded |
| `ats_offer` | the letter was issued or re-issued, the candidate responded, the planned start or salary was corrected, or the offer was rescinded / reneged |
| `hr_worker_event` | the HR record was created or corrected (payroll records a start or an exit a few days after it happens) |
| the three lookup files | a source label or code last changed (`timestamps.reference_updated_at`) |

Loading follows the contract: a **complete extract** is the default. Incremental upserts
may match on the source keys and replace a row only for a newer `updated_at`; they cannot
detect a disappearing row, so offer coverage is compared, never inferred.

## `ats_business_unit`, `ats_job_family`, `ats_job_level`

Organisation reference data attached to requisitions. dbt adds surrogate keys and the
`Unknown` member (key -1) required by the dimension contracts.

| Table | Columns |
|---|---|
| `ats_business_unit` | `business_unit_code` (natural key), `business_unit_name`, `sort_order`, `is_active`, `updated_at`, `extracted_at` |
| `ats_job_family` | `job_family_code`, `job_family_name`, `sort_order`, `is_active`, `updated_at`, `extracted_at` |
| `ats_job_level` | `job_level_code`, `job_level_name`, `level_rank` (1 = most junior), `is_active`, `updated_at`, `extracted_at` |

## `ats_requisition_snapshot`

**Grain:** one row per requisition per monthly extract (`requisition_id`, `snapshot_date`
unique). **Every requisition appears in the as-of extract**, as the contract requires;
earlier month-end rows are the retained history, kept while the requisition was open and
for 120 days after it closed. dbt must resolve **one row per requisition: the latest
`snapshot_date` on or before the as-of date** (`int_requisition__resolved_snapshot`).

| Column | Type | Description |
|---|---|---|
| `snapshot_date` | date | Extract date (month-end, <= as-of date). |
| `requisition_id` | string | Natural key, `REQ-YYYY-NNNNN` (year of approval). |
| `requisition_title` | string | *Source realism.* Level plus role title, e.g. "Senior Software Engineer". |
| `business_unit_code`, `job_family_code`, `job_level_code` | string | Organisation attributes; keys into the reference tables. Inherited downstream by applications, stage events and hires. |
| `work_location`, `hiring_manager_id`, `recruiter_id` | string | *Source realism.* Not used by the dashboard. |
| `requisition_status` | string | State on the snapshot date: `open` (openings > 0), `filled` (openings = 0), `cancelled`. A requisition can go `filled` -> `open` after a post-acceptance loss. |
| `approval_date` | date | Requisition approval date; start of Time to Fill. Constant across snapshots. |
| `target_hire_date` | date | THD on the snapshot date. Can be pushed out (re-baselined) while a requisition is open past its TOAD; never moves earlier. May be after the as-of date, never after 2027-05-31. |
| `target_offer_acceptance_date` | date | TOAD on the snapshot date, always between `approval_date` and `target_hire_date`. **Use as provided; never recompute from THD.** Moves together with THD when re-baselined. |
| `requested_positions` | integer | Net active demand: seats the business still expects TA to fill. Cancelled seats are already removed; a fully cancelled requisition shows 0. |
| `openings_position` | integer | Seats not held by an active accepted offer on the snapshot date. `requested_positions = active fills + openings_position` holds on every non-cancelled snapshot. |
| `cancelled_positions` | integer | Seats removed from demand so far (partial or full cancellation). `requested_positions + cancelled_positions` is constant per requisition. |
| `hiring_constraint_code` | string | Recruiter-recorded primary constraint: `qualified_candidates`, `hiring_manager_delay`, `compensation`, `interview_capacity`, `niche_skills`, `candidate_availability`, `no_material_constraint`. Cancelled requisitions carry `no_material_constraint`. |
| `updated_at`, `extracted_at` | timestamp | See above. |

## `ats_application`

**Grain:** one row per application; `application_id` unique and (`candidate_id`,
`requisition_id`) unique. A candidate may apply to several requisitions.

| Column | Type | Description |
|---|---|---|
| `application_id` | string | `APP-NNNNNNN`, ordered by application date. |
| `candidate_id` | string | `CAND-NNNNNNN`. Degenerate key; no candidate table is produced. |
| `requisition_id` | string | Requisition applied to. |
| `application_date` | date | Application date; equals the entry date of the first stage. Never before the requisition approval date. |
| `source_channel` | string | *Source realism.* `career_site`, `job_board`, `linkedin`, `referral`, `agency`, `internal`, `sourced`. |
| `application_status_current` | string | Mutable ATS status on the as-of date: `active` (still in process on an open requisition), `rejected`, `withdrawn` (candidate left with no offer), `offer_declined` (candidate declined before accepting), `offer_withdrawn` (employer withdrew before acceptance), `offer_accepted` (accepted, not started yet), `started` (the person actually started), `offer_rescinded` (employer rescinded after acceptance), `candidate_renege` (candidate withdrew after acceptance). **Never use this to decide whether an offer was accepted** — the acceptance event is the dated `offer_accepted_date` on `ats_offer`. |
| `current_stage_code` | string | Last stage entered: `review`, `screen`, `assessment`, `interview`, `offer`. Any application that reached acceptance stays in `offer`. |
| `rejected_date` | date | Set exactly when the status is `rejected`. |
| `withdrawal_date` | date | Set exactly when the status is `withdrawn` (a pre-acceptance candidate withdrawal with no offer on the table). |
| `disposition_reason` | string | Required for `rejected` / `withdrawn`: e.g. `not_qualified`, `interview_feedback`, `position_filled`, `requisition_cancelled`, `accepted_other_offer`. Null otherwise. |
| `updated_at`, `extracted_at` | timestamp | See above. |

Offer, decline, rescind, renege and start dates are **not** repeated here: they live on
`ats_offer` and `hr_worker_event`, which is what keeps one dated event in one place.

## `ats_stage_history`

**Grain:** one row per application per stage entered; (`application_id`,
`stage_sequence_number`) unique. The flow is linear (`review` -> `screen` -> `assessment`
-> `interview` -> `offer`) with no skips and no returns. dbt derives conversion, days in
stage and active stage age (`int_stage_event__sequenced`).

| Column | Type | Description |
|---|---|---|
| `stage_event_id` | string | `STG-NNNNNNNN`. |
| `application_id` | string | Application. |
| `stage_code` | string | Stage entered. |
| `stage_sequence_number` | integer | 1 for the first stage; equals the stage's position in the flow. |
| `stage_entry_date` | date | Entry date; equals the exit date of the previous stage. |
| `stage_exit_date` | date | Exit date, or null while the application is still in the stage. Exactly the `active` applications have one open row. |
| `exit_reason` | string | Why the application left the process **from this stage**. Null when it advanced, when the stage is still open, and when the Offer stage was closed by an acceptance — an acceptance is a successful exit. Only pre-acceptance losses appear: `rejected`, `withdrawn`, `offer_declined`, `offer_withdrawn`. A post-acceptance rescind or renege is **not** a stage exit and never rewrites this column. |
| `updated_at`, `extracted_at` | timestamp | See above. |

## `ats_offer`

**Grain:** one row per application with an issued offer; `application_id` is the key.
This is a **current-state** extract: the offer as it stands on `extracted_at`. There is no
offer version or cycle identifier, and one application can never hold two acceptances. An
application with no offer has no row here.

| Column | Type | Description |
|---|---|---|
| `application_id` | string | Key of this offer row. |
| `requisition_id` | string | The application's requisition; always matches `ats_application`. |
| `offer_status_current` | string | Current state: `pending` (issued, awaiting a response on the as-of date), `accepted` (**includes both pending starts and people who already started**), `offer_declined` (candidate, before acceptance), `offer_withdrawn` (employer, before acceptance), `offer_rescinded` (employer, after acceptance), `candidate_renege` (candidate, after acceptance). |
| `offer_extended_date` | date | Date the current letter was issued. Required on every row. A letter re-issued after a re-negotiation carries the revised date, while the Offer stage entry in `ats_stage_history` keeps the date the first letter went out. |
| `offer_accepted_date` | date | The acceptance event. **Preserved after a later rescind or renege** — a rule that nulls it is a defect. Null for `pending`, `offer_declined` and `offer_withdrawn`. |
| `offer_declined_date`, `offer_withdrawn_date` | date | Pre-acceptance losses; never present together with an acceptance. |
| `offer_rescinded_date`, `candidate_renege_date` | date | Post-acceptance losses, mutually exclusive, always on or after the acceptance date. |
| `planned_start_date` | date | Start date agreed in the letter; may be after the as-of date. The **actual** start is the HR `start` event, which can differ. |
| `base_salary`, `currency` | integer, string | *Source realism.* Offer terms. A corrected salary is an edit to this row and advances `updated_at`; it never creates a second offer. |
| `updated_at`, `extracted_at` | timestamp | See above. |

**Reading guidance for dbt**

* Read the offer row by `application_id` and join to the application. There is nothing to
  resolve, no cycle to choose and no audit model to build.
* All issued offers are present, including declined, withdrawn, rescinded and reneged ones.
  A missing offer means the application never received one — **never** infer a loss from
  absence.
* A duplicate `application_id` inside one extract is a validation failure, not a second
  acceptance. Repeated rows across dated extracts are snapshots of the same record.
* A revision (moved planned start, corrected salary, re-issued letter) changes this row and
  advances `updated_at`. In the default run 921 accepted offers carry such an edit.

## `hr_worker_event`

**Grain:** one row per HR event. Event-shaped, with realistic integration noise: about 2% of
start events are re-sent as exact duplicates with a new `worker_event_id`, and about 3% of
terminations carry a second row dated a few days later. dbt keeps one start per application
and the **earliest termination after the start** (`fct_hire_outcome`).

| Column | Type | Description |
|---|---|---|
| `worker_event_id` | string | `WE-NNNNNNN`. |
| `worker_id` | string | HR worker identifier, one per started person; links the start and termination of one employment spell. |
| `candidate_id`, `requisition_id` | string | *Source realism.* Both are reachable through `application_id`. |
| `application_id` | string | The ATS application whose accepted offer produced the start. Every start references an application with `application_status_current = started`. |
| `event_type` | string | `start` (actual first day of employment) or `termination`. |
| `event_date` | date | Start date or termination date; on or before the as-of date; a start is on or after the offer acceptance date. |
| `termination_reason` | string | Required for `termination` (`voluntary_resignation`, `involuntary_performance`, `involuntary_probation`, `involuntary_restructuring`), **null for a start**. |
| `updated_at`, `extracted_at` | timestamp | See above. `updated_at` is when HR created or corrected the record, so it is normally a few days after `event_date`. |

Accepted offers that have not started yet have **no** row here. That gap is normal
pipeline, and it is what makes Positions Filled larger than Started Hires.

## What dbt derives from these files (and this repository does not)

`is_offer_accepted_event`, `is_active_fill`, `is_started`, `post_acceptance_outcome`,
`time_to_fill_days`; `filled_positions`, `accepted_offer_events`,
`lost_after_acceptance_positions`, `started_positions`; `days_to_toad`, `risk_band_code`,
`is_at_risk`; `advanced_to_next_stage`, `days_in_stage`, `active_stage_age_days`; cohort
maturity and the rolling-12 window; stage-to-active-fill yield and the capped forecast.
