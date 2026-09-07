# TA Exec Data Generator

Deterministic synthetic **ATS and HR source data** for the Talent Acquisition Executive
Dashboard. The dashboard contract (specification, wireframe, metric definitions, dbt
ownership rules and schema contracts) lives in
[`bremlydvillasenor/ta-exec-db`](https://github.com/bremlydvillasenor/ta-exec-db). A separate
dbt repository turns these raw files into the dimensions, facts and marts that contract
describes.

**Implements contract release 1.3**, commit
[`87f8cf2`](https://github.com/bremlydvillasenor/ta-exec-db/commit/87f8cf26795e0d15c1a57f14bf7fb485e959fcc5).
Every run records that release and commit in `data/raw/manifest.json`, next to the seed,
the effective configuration, per-file checksums and the validation result.

This repository only simulates the **source systems**. It writes dated events, statuses,
quantities and attributes. It never decides what a record means: no active-fill flags, no
risk bands, no cohort maturity, no stage yields, no KPIs. Those derivations belong to dbt.

## Reporting boundaries (from the contract)

| Setting | Value |
|---|---|
| Historical activity begins | 2024-01-01 |
| Reporting as-of date | 2026-05-31 (no actual event after this day) |
| Future Target Hire Dates preserved through | 2027-05-31 (demand runs to this day) |
| Random seed (default) | 20260531 |
| Extraction timestamp on every row (`extracted_at`) | 2026-05-31T23:59:59Z |

## Quick start

```bash
uv sync --all-groups                 # Python 3.11+, installs polars, numpy, pyyaml, pydantic (+ pytest, ruff)
uv run ta-gen generate               # writes data/raw/*.csv and runs source validation (about 10 s)
uv run ta-gen validate               # re-runs the 127 source-level checks on data/raw
uv run ta-gen summary                # prints the indicative data-story summary (never written to the outputs)
uv run ta-gen fixtures               # rewrites the deliberately invalid extracts under data/fixtures/invalid
uv run pytest                        # 41 tests on scaled-down runs (about 20 s)
uv run ruff check src tests
```

Options: `--config path.yaml` (default `config/default.yaml`), `--output dir`, `--seed N`,
`--skip-validation`, `-v`. The same seed and configuration - including the configured
extraction timestamp - always produce byte-identical files.

## Generated files (`data/raw/`, default configuration)

Every file carries `updated_at` (when that source record last changed) and `extracted_at`
(when the batch was exported). File names keep their source-system prefix; the mapping onto
the contract's logical file names is in the data dictionary.

| File | Contract file | Grain | Rows | dbt staging model |
|---|---|---|---:|---|
| `ats_business_unit.csv` | `business_units.csv` | one row per business unit | 6 | source for `dim_business_unit` |
| `ats_job_family.csv` | `job_families.csv` | one row per job family | 10 | source for `dim_job_family` |
| `ats_job_level.csv` | `job_levels.csv` | one row per job level | 6 | source for `dim_job_level` |
| `ats_requisition_snapshot.csv` | `requisition_snapshots.csv` | one row per requisition per month-end extract; every requisition is in the as-of extract | 22,335 | `stg_ats__requisition_snapshot` |
| `ats_application.csv` | `applications.csv` | one row per application (candidate x requisition) | 74,131 | `stg_ats__application` |
| `ats_stage_history.csv` | `stage_history.csv` | one row per application per stage entered | 155,899 | `stg_ats__stage_history` |
| `ats_offer.csv` | `offers.csv` | one **current** offer row per application with an issued offer | 5,042 | `stg_ats__offer` |
| `hr_worker_event.csv` | `worker_events.csv` | one row per HR start / termination event | 4,018 | `stg_hr__worker_event` |

`data/fixtures/invalid/` holds one deliberately broken extract per documented violation, so
the source validation can be shown to catch each failure mode. It is never loaded by dbt.

Column-level definitions are in [`docs/data_dictionary.md`](docs/data_dictionary.md). The
intended executive story and the numbers it produces are in
[`docs/data_story.md`](docs/data_story.md), including a reconciliation against the
wireframe's illustrative figures. Design decisions, assumptions and the points where the
contract needed interpretation are in [`docs/design.md`](docs/design.md), which also lists
the open questions this data raises for `ta-exec-db` - chief among them that the risk
visual's default Target Hire Date selection cannot show anything but the Missed band.

## How the data is produced

1. **Demand plan** (`requisitions.py`): a monthly curve of positions by Target Hire Date
   (base level, growth, seasonality, a hiring surge) is split into requisitions with a
   business unit, job family, job level, seat count, approval date and Target Offer
   Acceptance Date, plus lifecycle plans (cancellation, partial cancellation, re-baselining)
   that only apply if the requisition is still open on the planned day.
2. **Funnel simulation** (`funnel.py`): each requisition is simulated chronologically.
   Candidates apply in a sourcing burst and a trickle, walk review -> screen -> assessment
   -> interview -> offer with segment-specific pass rates and durations, and seats are
   filled in acceptance order. A full requisition closes its posting and dispositions the
   remaining pipeline. Post-acceptance reneges and rescinds reopen the seat and start a new
   sourcing wave. Everything after the as-of date is cut off, which is what leaves an active
   pipeline and accepted offers still waiting to start.
3. **Current offers** (`offers.py`): one row per application with an issued offer, carrying
   its current status and every dated event that happened. A re-negotiated letter before
   the response, or a moved start date or corrected salary after acceptance, edits that row
   and advances its `updated_at` - it never creates a second offer or a second acceptance.
   Rescinded and reneged offers stay in the extract with their acceptance date preserved.
4. **HR events** (`hr.py`): `start` events for actual starts, early and later terminations,
   plus duplicate integration rows that dbt must de-duplicate.
5. **Snapshots** (`snapshots.py`): month-end requisition extracts with status, requested /
   open / cancelled seats, re-baselined target dates and a primary hiring constraint chosen
   from evidence in the pipeline. The as-of extract covers every requisition, including
   ones closed long before it.
6. **Timestamps** (`timestamps.py`): `extracted_at` from the configuration, and `updated_at`
   from the day each record actually changed. Re-exporting an unchanged row repeats its
   timestamp; a real change advances it, never backwards.
7. **Validation** (`validate.py`): 127 source-level checks (keys inside the extract,
   referential integrity, date order, nothing after the as-of date, the raw timestamp
   rules, seat identity on every snapshot, offer / status consistency, HR consistency, and
   candidate realism - nobody holds two live acceptances, is started twice, or keeps
   applying after taking a seat).

All behaviour is configured in `config/default.yaml`. Randomness comes from named numpy
streams derived from one seed (`rng.py`), so changing one module's draws does not reshuffle
another's.

## Project layout

```
config/default.yaml            all story and volume parameters
src/ta_exec_data_gen/
  cli.py                       ta-gen generate | validate | summary | fixtures
  config.py                    pydantic configuration model
  pipeline.py                  orchestration and final table assembly
  requisitions.py  funnel.py  offers.py  hr.py  snapshots.py  reference.py
  timestamps.py                updated_at / extracted_at rules
  fixtures.py                  deliberately invalid extracts
  validate.py                  source-level checks
  story.py                     indicative summary incl. the FCST-01..04 forecast
                               (documentation and tests only)
  writer.py  rng.py  dates.py
tests/                         pytest suite (determinism, validation, contract alignment, story)
data/raw/                      generated CSV outputs and manifest.json
data/fixtures/invalid/         deliberately invalid extracts (never loaded by dbt)
docs/                          data dictionary, data story, design notes
```
