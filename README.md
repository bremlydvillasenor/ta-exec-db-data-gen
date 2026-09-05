# TA Exec Data Generator

Deterministic synthetic **ATS and HR source data** for the Talent Acquisition Executive
Dashboard. The dashboard contract (specification, wireframe, metric definitions, dbt
ownership rules and schema contracts) lives in
[`bremlydvillasenor/ta-exec-db`](https://github.com/bremlydvillasenor/ta-exec-db). A separate
dbt repository turns these raw files into the dimensions, facts and marts that contract
describes.

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

## Quick start

```bash
uv sync --all-groups                 # Python 3.11+, installs polars, numpy, pyyaml, pydantic (+ pytest, ruff)
uv run ta-gen generate               # writes data/raw/*.csv and runs source validation (about 10 s)
uv run ta-gen validate               # re-runs the 83 source-level checks on data/raw
uv run ta-gen summary                # prints the indicative data-story summary (never written to the outputs)
uv run pytest                        # 36 tests on scaled-down runs (about 15 s)
uv run ruff check src tests
```

Options: `--config path.yaml` (default `config/default.yaml`), `--output dir`, `--seed N`,
`--skip-validation`, `-v`. The same seed and configuration always produce byte-identical
files; `data/raw/_manifest.json` records the seed, a configuration fingerprint and row
counts.

## Generated files (`data/raw/`, default configuration)

| File | Grain | Rows | dbt staging model (per `dbt-ownership.md`) |
|---|---|---:|---|
| `ats_business_unit.csv` | one row per business unit | 6 | source for `dim_business_unit` |
| `ats_job_family.csv` | one row per job family | 10 | source for `dim_job_family` |
| `ats_job_level.csv` | one row per job level | 6 | source for `dim_job_level` |
| `ats_requisition_snapshot.csv` | one row per requisition per month-end extract | 20,117 | `stg_ats__requisition_snapshot` |
| `ats_application.csv` | one row per application (candidate x requisition) | 74,131 | `stg_ats__application` |
| `ats_stage_history.csv` | one row per application per stage entered | 155,899 | `stg_ats__stage_history` |
| `ats_offer_version.csv` | one row per offer version | 6,631 | `stg_ats__offer_version` |
| `hr_worker_event.csv` | one row per HR hire / termination event | 4,018 | `stg_hr__worker_event` |

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
3. **Offer versions** (`offers.py`): negotiation revisions, administrative revisions after
   acceptance (several accepted versions of one offer), and a few ambiguous second offer
   cycles for the dbt audit model.
4. **HR events** (`hr.py`): hire events for actual starts, early and later terminations,
   plus duplicate integration rows that dbt must de-duplicate.
5. **Snapshots** (`snapshots.py`): month-end requisition extracts with status, requested /
   open / cancelled seats, re-baselined target dates and a primary hiring constraint chosen
   from evidence in the pipeline.
6. **Validation** (`validate.py`): 83 source-level checks (keys, referential integrity, date
   order, nothing after the as-of date, seat identity on every snapshot, offer / status
   consistency, HR consistency, and candidate realism - nobody holds two live acceptances,
   is hired twice, or keeps applying after taking a seat).

All behaviour is configured in `config/default.yaml`. Randomness comes from named numpy
streams derived from one seed (`rng.py`), so changing one module's draws does not reshuffle
another's.

## Project layout

```
config/default.yaml            all story and volume parameters
src/ta_exec_data_gen/
  cli.py                       ta-gen generate | validate | summary
  config.py                    pydantic configuration model
  pipeline.py                  orchestration and final table assembly
  requisitions.py  funnel.py  offers.py  hr.py  snapshots.py  reference.py
  validate.py                  source-level checks
  story.py                     indicative summary incl. the FCST-01..04 forecast
                               (documentation and tests only)
  writer.py  rng.py  dates.py
tests/                         pytest suite (determinism, validation, contract alignment, story)
data/raw/                      generated CSV outputs and _manifest.json
docs/                          data dictionary, data story, design notes
```
