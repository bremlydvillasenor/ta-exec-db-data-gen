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
    }
    for name, (_, expected) in INVALID_CASES.items():
        lines.append(f"| `{name}` | {descriptions[name]} | contains `{expected}` |")
    lines.append("")
    return "\n".join(lines)


__all__ = ["INVALID_CASES", "build_invalid_fixtures", "load_config_like", "write_invalid_fixtures"]
