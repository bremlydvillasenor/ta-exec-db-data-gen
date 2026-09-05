# Source data dictionary

Raw ATS and HR extracts generated for the TA Executive Dashboard. Column names, grains and
vocabularies are aligned with the `ta-exec-db` contract, but every table here is a
**source** table: dbt derives the governed flags, keys and measures from these dates,
statuses and quantities. Dates are ISO `YYYY-MM-DD`, booleans are `true` / `false`, nulls
are empty strings.

## `ats_business_unit`, `ats_job_family`, `ats_job_level`

Organisation reference data attached to requisitions. dbt adds surrogate keys and the
`Unknown` member (key -1) required by the dimension contracts.

| Table | Columns |
|---|---|
| `ats_business_unit` | `business_unit_code` (natural key), `business_unit_name`, `sort_order`, `is_active` |
| `ats_job_family` | `job_family_code`, `job_family_name`, `sort_order`, `is_active` |
| `ats_job_level` | `job_level_code`, `job_level_name`, `level_rank` (1 = most junior), `is_active` |

## `ats_requisition_snapshot`

**Grain:** one row per requisition per monthly extract (`requisition_id`, `snapshot_date`
unique). Extracts are taken on every month-end from the approval month to the as-of date,
and stop 120 days after a requisition closes. dbt must resolve **one row per requisition:
the latest `snapshot_date` on or before the as-of date** (`int_requisition__resolved_snapshot`).

| Column | Type | Description |
|---|---|---|
| `snapshot_date` | date | Extract date (month-end, <= as-of date). |
| `requisition_id` | string | Natural key, `REQ-YYYY-NNNNN` (year of approval). |
| `requisition_title` | string | Level plus role title, e.g. "Senior Software Engineer". |
| `business_unit_code`, `job_family_code`, `job_level_code` | string | Organisation attributes; keys into the reference tables. Inherited downstream by applications, stage events and hires. |
| `work_location`, `hiring_manager_id`, `recruiter_id` | string | Descriptive attributes, not used by the dashboard. |
| `requisition_status` | string | State on the snapshot date: `open` (openings > 0), `filled` (openings = 0), `cancelled`. A requisition can go `filled` -> `open` after a post-acceptance loss. |
| `approval_date` | date | Requisition approval date; start of Time to Fill. Constant across snapshots. |
| `target_hire_date` | date | THD on the snapshot date. Can be pushed out (re-baselined) while a requisition is open past its TOAD; never moves earlier. May be after the as-of date, never after 2027-05-31. |
| `target_offer_acceptance_date` | date | TOAD on the snapshot date, always between `approval_date` and `target_hire_date`. **Use as provided; never recompute from THD.** Moves together with THD when re-baselined. |
| `requested_positions` | integer | Net active demand: seats the business still expects TA to fill. Cancelled seats are already removed; a fully cancelled requisition shows 0. |
| `openings_position` | integer | Seats not held by an active accepted offer on the snapshot date. `requested_positions = active fills + openings_position` holds on every non-cancelled snapshot. |
| `cancelled_positions` | integer | Seats removed from demand so far (partial or full cancellation). `requested_positions + cancelled_positions` is constant per requisition. |
| `primary_hiring_constraint` | string | Recruiter-recorded primary constraint: `qualified_candidates`, `hiring_manager_delay`, `compensation`, `interview_capacity`, `niche_skills`, `candidate_availability`, `no_material_constraint`. Cancelled requisitions carry `no_material_constraint`. |

## `ats_application`

**Grain:** one row per application; `application_id` unique and (`candidate_id`,
`requisition_id`) unique. A candidate may apply to several requisitions.

| Column | Type | Description |
|---|---|---|
| `application_id` | string | `APP-NNNNNNN`, ordered by application date. |
| `candidate_id` | string | `CAND-NNNNNNN`. Degenerate key; no candidate table is produced. |
| `requisition_id` | string | Requisition applied to. |
| `application_date` | date | Application date; equals the entry date of the first stage. Never before the requisition approval date. |
| `source_channel` | string | `career_site`, `job_board`, `linkedin`, `referral`, `agency`, `internal`, `sourced`. Descriptive only. |
| `application_status` | string | ATS status on the as-of date. `active` (still in process on an open requisition), `rejected`, `withdrawn` (candidate left with no offer), `offer_declined` (candidate declined before accepting), `offer_withdrawn` (employer withdrew before acceptance), `offer_accepted` (accepted, no post-acceptance loss recorded), `offer_rescinded` (employer rescinded after acceptance), `candidate_renege` (candidate withdrew after acceptance). The ATS has **no `started` or `hired` status**; dbt derives `started` from `hr_worker_event`. |
| `status_date` | date | Date of the last status change. For `active` rows, the entry date of the current stage. |
| `current_stage_code` | string | Last stage entered: `review`, `screen`, `assessment`, `interview`, `offer`. Any application that reached acceptance stays in `offer`. |
| `disposition_reason` | string | Only for `rejected` / `withdrawn`: e.g. `not_qualified`, `interview_feedback`, `position_filled`, `requisition_cancelled`, `accepted_other_offer`. Null otherwise. |

## `ats_stage_history`

**Grain:** one row per application per stage entered; (`application_id`, `stage_sequence`)
unique. The flow is linear (`review` -> `screen` -> `assessment` -> `interview` ->
`offer`) with no skips and no returns. dbt derives stage exit outcome, conversion, days in
stage and active stage age (`int_stage_event__sequenced`).

| Column | Type | Description |
|---|---|---|
| `stage_history_id` | string | `STG-NNNNNNNN`. |
| `application_id` | string | Application. |
| `stage_code` | string | Stage entered. |
| `stage_sequence` | integer | 1 for the first stage; equals the stage's position in the flow. |
| `stage_entered_date` | date | Entry date; equals the exit date of the previous stage. |
| `stage_exited_date` | date | Exit date, or null while the application is still in the stage. Exactly the `active` applications have one open row. An accepted offer exits the offer stage on the acceptance date; a later rescind or renege does not change the row. |

## `ats_offer_version`

**Grain:** one row per offer version; `offer_version_id` unique. `offer_id` identifies an
offer **cycle** (one offer made to one application); `offer_version_number` counts the
versions of that cycle. dbt resolves **one governed acceptance per application**
(`int_offer__resolved_acceptance`) and records multi-accepted cases in
`audit_offer__multi_accepted_version`.

| Column | Type | Description |
|---|---|---|
| `offer_version_id` | string | `OFR-NNNNNN-Vk`. |
| `offer_id` | string | Offer cycle. Almost every application has one; a handful carry two (see below). |
| `application_id`, `requisition_id` | string | Application and its requisition. |
| `offer_cycle_number` | integer | 1 for the first cycle on the application, 2 for a second cycle. |
| `offer_version_number` | integer | 1..n within the cycle, contiguous. |
| `version_reason` | string | `initial`, `negotiation_revision` (revised before the candidate responded), `start_date_revision`, `salary_correction`, `letter_reissue` (administrative revisions after acceptance). |
| `offer_status` | string | `extended` (awaiting response on the as-of date), `superseded` (replaced by a later version before response), `accepted`, `declined`, `withdrawn` (employer, before acceptance), `rescinded` (employer, after acceptance), `reneged` (candidate, after acceptance). |
| `is_current_version` | boolean | True for the latest version of each cycle. |
| `offer_extended_date` | date | Date this version was issued. |
| `offer_accepted_date` | date | Acceptance date recorded on this version. Administrative revisions re-record the acceptance with a later date, so one cycle can hold several accepted versions. **The governed acceptance is the earliest accepted date of the cycle.** Preserved after a rescind or renege. |
| `offer_declined_date`, `offer_withdrawn_date` | date | Pre-acceptance losses; mutually exclusive with an acceptance on the same version. |
| `offer_rescinded_date`, `candidate_renege_date` | date | Post-acceptance losses, recorded on the current version, always on or after the acceptance date. |
| `proposed_start_date` | date | Planned start date on this version; may be after the as-of date. The actual start is the HR hire event. |
| `base_salary`, `currency` | integer, string | Offer terms; descriptive. |

**Resolution guidance for dbt**

* Several accepted versions with the **same `offer_id`** are administrative revisions of one
  acceptance: collapse them and keep the earliest `offer_accepted_date`
  (`resolution = administrative_revision`).
* Accepted versions on **different `offer_id`s** of the same application are two distinct
  acceptance cycles recorded on one application (an ATS misuse: the source should have
  created a new application). These are ambiguous and must be **quarantined**
  (`resolution = quarantined`). The default configuration generates 4 such applications;
  both of their cycles end in a renege, so no open seat depends on how they are resolved.

## `hr_worker_event`

**Grain:** one row per HR event. Event-shaped, with realistic integration noise: about 2% of
hire events are re-sent as exact duplicates with a new `worker_event_id`, and about 3% of
terminations carry a second row dated a few days later. dbt keeps one hire per application
and the **earliest termination after the start** (`fct_hire_outcome`).

| Column | Type | Description |
|---|---|---|
| `worker_event_id` | string | `WE-NNNNNNN`. |
| `worker_id` | string | HR worker identifier, one per started person. |
| `candidate_id`, `application_id`, `requisition_id` | string | Link back to the ATS application whose accepted offer produced the start. Every hire references an application with `application_status = offer_accepted`. |
| `event_type` | string | `hire` (actual first day of employment) or `termination`. |
| `event_date` | date | Start date or termination date; on or before the as-of date; start on or after the offer acceptance date. |
| `event_reason` | string | `new_hire`; terminations: `voluntary_resignation`, `involuntary_performance`, `involuntary_probation`, `involuntary_restructuring`. |
| `record_created_date` | date | When the record reached the HR extract; later than `event_date` for duplicates. |

## What dbt derives from these files (and this repository does not)

`application_status_current` including `started`; `is_offer_accepted_event`,
`is_active_fill`, `is_started`, `post_acceptance_outcome`, `time_to_fill_days`;
`filled_positions`, `accepted_offer_events`, `lost_after_acceptance_positions`,
`started_positions`; `days_to_toad`, `risk_band_code`, `is_at_risk`; `advanced_to_next_stage`,
`exit_reason`, `days_in_stage`, `active_stage_age_days`; cohort maturity and the rolling-12
window; stage-to-active-fill yield and the capped forecast.
