"""Deliberately invalid extracts, kept out of the normal outputs.

The contract asks for the invalid cases to live in "a small separate fixture directory".
Each case starts from a valid scaled-down batch and applies exactly one documented
violation, so a reviewer can see that the validator catches the failure mode rather than
taking it on trust. The tests assert that each fixture trips its own named check and that
the untouched batch passes everything.

These files are never part of `data/raw` and must never be loaded by dbt.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from pathlib import Path

import polars as pl

from .config import GeneratorConfig
from .pipeline import generate
from .writer import write_manifest, write_tables

Tables = dict[str, pl.DataFrame]
Mutation = Callable[[Tables, GeneratorConfig], Tables]


def _null_first_value(table: str, column: str) -> Mutation:
    """Blank one required value - the failure mode a comparison-based check cannot see."""

    def mutate(tables: Tables, cfg: GeneratorConfig) -> Tables:
        frame = tables[table]
        tables[table] = frame.with_columns(
            pl.when(pl.int_range(pl.len()) == 0).then(None).otherwise(pl.col(column)).alias(column)
        )
        return tables

    return mutate


def _stale_updated_at(tables: Tables, cfg: GeneratorConfig) -> Tables:
    """A stage exit recorded without advancing updated_at, so an incremental load misses it."""
    stg = tables["ats_stage_history"]
    target = stg.filter(pl.col("stage_exit_date").is_not_null()).head(1)["stage_event_id"].to_list()
    tables["ats_stage_history"] = stg.with_columns(
        updated_at=pl.when(pl.col("stage_event_id").is_in(target))
        .then(pl.col("stage_entry_date").cast(pl.Datetime("us")))
        .otherwise(pl.col("updated_at"))
    )
    return tables


def _overlapping_repeat_attempt(tables: Tables, cfg: GeneratorConfig) -> Tables:
    """A second attempt at the same requisition submitted before the first one ended."""
    app = tables["ats_application"]
    pairs = app.group_by("candidate_id", "requisition_id").len().filter(pl.col("len") > 1)
    if pairs.height == 0:  # pragma: no cover - the default configuration always has repeats
        raise ValueError("no repeated candidate/requisition attempt to make overlap")
    key = pairs.head(1).row(0, named=True)
    rows = app.filter(
        (pl.col("candidate_id") == key["candidate_id"]) & (pl.col("requisition_id") == key["requisition_id"])
    ).sort("application_date")
    later = [rows["application_id"][1]]
    tables["ats_application"] = app.with_columns(
        application_date=pl.when(pl.col("application_id").is_in(later))
        .then(pl.lit(rows["application_date"][0]))
        .otherwise(pl.col("application_date"))
    )
    return tables


def _duplicate_offer_key(tables: Tables, cfg: GeneratorConfig) -> Tables:
    """Two rows for one application in one extract: a duplicate key, never a second offer."""
    off = tables["ats_offer"]
    tables["ats_offer"] = pl.concat([off, off.head(1)])
    return tables


def _acceptance_erased_by_renege(tables: Tables, cfg: GeneratorConfig) -> Tables:
    """A renege that clears offer_accepted_date - the defect the contract calls out by name."""
    off = tables["ats_offer"]
    target = off.filter(pl.col("candidate_renege_date").is_not_null()).head(1)["application_id"].to_list()
    tables["ats_offer"] = off.with_columns(
        offer_accepted_date=pl.when(pl.col("application_id").is_in(target))
        .then(pl.lit(None, dtype=pl.Date))
        .otherwise(pl.col("offer_accepted_date"))
    )
    return tables


def _event_after_as_of(tables: Tables, cfg: GeneratorConfig) -> Tables:
    """An actual HR start dated after the reporting as-of date."""
    hr = tables["hr_worker_event"]
    tables["hr_worker_event"] = hr.with_columns(
        event_date=pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(cfg.dates.as_of + dt.timedelta(days=30)))
        .otherwise(pl.col("event_date"))
    )
    return tables


def _updated_after_extracted(tables: Tables, cfg: GeneratorConfig) -> Tables:
    """A change timestamp later than the extract that supposedly contains it."""
    app = tables["ats_application"]
    tables["ats_application"] = app.with_columns(
        updated_at=pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.col("extracted_at") + pl.duration(days=2))
        .otherwise(pl.col("updated_at"))
    )
    return tables


def _seat_identity_broken(tables: Tables, cfg: GeneratorConfig) -> Tables:
    """Open seats that no longer reconcile with requested seats minus active fills."""
    snap = tables["ats_requisition_snapshot"]
    tables["ats_requisition_snapshot"] = snap.with_columns(
        openings_position=pl.when(pl.col("requisition_status") == "open")
        .then(pl.col("openings_position") + 1)
        .otherwise(pl.col("openings_position"))
    )
    return tables


# case name -> (mutation, the substring of the validator check it must trip)
INVALID_CASES: dict[str, tuple[Mutation, str]] = {
    "duplicate_offer_key": (_duplicate_offer_key, "unique application_id"),
    "acceptance_erased_by_renege": (_acceptance_erased_by_renege, "keeps its acceptance date"),
    "hr_event_after_as_of": (_event_after_as_of, "event_date <= as_of"),
    "updated_at_after_extracted_at": (_updated_after_extracted, "updated_at <= extracted_at"),
    "seat_identity_broken": (_seat_identity_broken, "requested = active fills + openings"),
    "missing_identifier": (
        _null_first_value("ats_application", "candidate_id"),
        "ats_application: required columns have no missing values",
    ),
    "missing_status": (
        _null_first_value("ats_offer", "offer_status_current"),
        "ats_offer: required columns have no missing values",
    ),
    "missing_date": (
        _null_first_value("hr_worker_event", "event_date"),
        "hr_worker_event: required columns have no missing values",
    ),
    "missing_quantity": (
        _null_first_value("ats_requisition_snapshot", "openings_position"),
        "ats_requisition_snapshot: required columns have no missing values",
    ),
    "missing_lookup_name": (
        _null_first_value("ats_business_unit", "business_unit_name"),
        "ats_business_unit: required columns have no missing values",
    ),
    "stale_updated_at": (_stale_updated_at, "stage_history: updated_at reflects its latest recorded date"),
    "overlapping_repeat_attempt": (
        _overlapping_repeat_attempt,
        "a repeated attempt starts after the previous one ended",
    ),
}


def build_invalid_fixtures(cfg: GeneratorConfig, tables: Tables) -> dict[str, Tables]:
    """Apply each documented violation to its own copy of a valid batch."""
    return {name: mutate(dict(tables), cfg) for name, (mutate, _) in INVALID_CASES.items()}


def write_invalid_fixtures(
    cfg: GeneratorConfig, out_dir: str | Path, *, base_positions: float = 1.0
) -> dict[str, Path]:
    """Generate a small valid batch, then write one directory per invalid case."""
    scaled = load_config_like(cfg, base_positions)
    tables = generate(scaled)
    out = Path(out_dir)
    written: dict[str, Path] = {}
    for name, broken in build_invalid_fixtures(scaled, tables).items():
        case_dir = out / name
        write_tables(broken, case_dir, scaled)
        write_manifest(
            broken,
            case_dir,
            scaled,
            validation={
                "status": "expected_failure",
                "case": name,
                "expected_check_contains": INVALID_CASES[name][1],
            },
        )
        written[name] = case_dir
    (out / "README.md").write_text(_readme(), encoding="utf-8")
    return written


def load_config_like(cfg: GeneratorConfig, base_positions: float) -> GeneratorConfig:
    """The same configuration at fixture scale, so the invalid batches stay small."""
    raw = cfg.model_dump(mode="json")
    raw["demand"]["base_positions_per_month"] = base_positions
    return GeneratorConfig.model_validate(raw)


def _readme() -> str:
    lines = [
        "# Deliberately invalid extracts",
        "",
        "Generated by `ta-gen fixtures`. Each directory is a complete but **broken** batch",
        "carrying exactly one documented violation. They exist to prove the source",
        "validation catches the failure mode. Never load them with dbt.",
        "",
        "| Case | Violation | Validator check it must trip |",
        "|---|---|---|",
    ]
    descriptions = {
        "duplicate_offer_key": "two offer rows for one application inside one extract",
        "acceptance_erased_by_renege": "a candidate renege that clears `offer_accepted_date`",
        "hr_event_after_as_of": "an actual HR start dated after the reporting as-of date",
        "updated_at_after_extracted_at": "`updated_at` later than the extract that contains it",
        "seat_identity_broken": "`requested_positions` no longer equals active fills plus openings",
        "missing_identifier": "a required identifier (`candidate_id`) left empty",
        "missing_status": "a required status (`offer_status_current`) left empty",
        "missing_date": "a required date (`event_date`) left empty",
        "missing_quantity": "a required quantity (`openings_position`) left empty",
        "missing_lookup_name": "a required lookup label (`business_unit_name`) left empty",
        "stale_updated_at": "a stage exit recorded without advancing `updated_at`",
        "overlapping_repeat_attempt": "a second attempt submitted before the first one ended",
    }
    for name, (_, expected) in INVALID_CASES.items():
        lines.append(f"| `{name}` | {descriptions[name]} | contains `{expected}` |")
    lines.append("")
    return "\n".join(lines)


__all__ = ["INVALID_CASES", "build_invalid_fixtures", "load_config_like", "write_invalid_fixtures"]
