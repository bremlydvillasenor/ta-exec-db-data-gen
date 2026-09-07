"""Write the raw source tables as CSV plus the run manifest the contract asks for.

The manifest is the handoff document: it names the contract release and commit this batch
implements, the generator commit that produced it, the seed and effective configuration
(including `extracted_at` and whether the source supplies a reliable `updated_at`), a row
count and checksum per file, and the validation outcome. A consumer can therefore tell
which contract a delivered dataset satisfies without reading generator internals.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import polars as pl

from .config import GeneratorConfig

MANIFEST_NAME = "manifest.json"
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def config_fingerprint(cfg: GeneratorConfig) -> str:
    payload = cfg.model_dump_json(exclude={"output"})
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def generator_commit() -> str | None:
    """The generator commit that produced this batch, when it is running from a checkout."""
    try:
        result = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parents[2]), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


def _checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_tables(tables: dict[str, pl.DataFrame], out_dir: str | Path, cfg: GeneratorConfig) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, frame in tables.items():
        frame.write_csv(
            out / f"{name}.csv",
            date_format=DATE_FORMAT,
            datetime_format=TIMESTAMP_FORMAT,
            null_value="",
        )
    return out


def write_manifest(
    tables: dict[str, pl.DataFrame],
    out_dir: str | Path,
    cfg: GeneratorConfig,
    *,
    validation: dict[str, object],
) -> Path:
    out = Path(out_dir)
    ts = cfg.timestamps
    manifest: dict[str, object] = {
        "contract": {
            "repository": cfg.contract.repository,
            "release": cfg.contract.release,
            "commit": cfg.contract.commit,
        },
        "generator": {
            "package": "ta-exec-data-gen",
            "commit": generator_commit(),
        },
        "seed": cfg.seed,
        "config_fingerprint": config_fingerprint(cfg),
        "as_of_date": cfg.dates.as_of.isoformat(),
        "history_start_date": cfg.dates.history_start.isoformat(),
        "future_thd_end_date": cfg.dates.future_thd_end.isoformat(),
        "extracted_at": ts.extracted_at.strftime(TIMESTAMP_FORMAT),
        "updated_at_available": ts.updated_at_available,
        "business_cutoff": f"{cfg.dates.as_of.isoformat()}T23:59:59Z",
        "extract_mode": "complete",
        "validation": validation,
        "effective_configuration": json.loads(cfg.model_dump_json()),
        "tables": {},
    }
    for name, frame in tables.items():
        path = out / f"{name}.csv"
        manifest["tables"][name] = {  # type: ignore[index]
            "file": path.name,
            "rows": frame.height,
            "columns": frame.columns,
            "sha256": _checksum(path),
        }
    (out / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return out / MANIFEST_NAME


def read_tables(in_dir: str | Path) -> dict[str, pl.DataFrame]:
    """Read the CSV outputs back with dates and timestamps parsed, for validation and summaries."""
    inp = Path(in_dir)
    manifest = json.loads((inp / MANIFEST_NAME).read_text(encoding="utf-8"))
    tables: dict[str, pl.DataFrame] = {}
    for name, meta in manifest["tables"].items():
        overrides: dict[str, pl.DataType] = {}
        for col in meta["columns"]:
            if col.endswith("_date"):
                overrides[col] = pl.Date
            elif col.endswith("_at"):
                overrides[col] = pl.Datetime("us")
            elif col.startswith("is_"):
                overrides[col] = pl.Boolean
            elif col.endswith("_id") or col.endswith("_code") or col in ("currency",):
                overrides[col] = pl.Utf8
        tables[name] = pl.read_csv(inp / f"{name}.csv", schema_overrides=overrides, null_values=[""])
    return tables
