"""Write the raw source tables as CSV plus a manifest with row counts and configuration hash."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl

from .config import GeneratorConfig


def config_fingerprint(cfg: GeneratorConfig) -> str:
    payload = cfg.model_dump_json(exclude={"output"})
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def write_tables(tables: dict[str, pl.DataFrame], out_dir: str | Path, cfg: GeneratorConfig) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, object] = {
        "seed": cfg.seed,
        "config_fingerprint": config_fingerprint(cfg),
        "as_of_date": cfg.dates.as_of.isoformat(),
        "history_start_date": cfg.dates.history_start.isoformat(),
        "future_thd_end_date": cfg.dates.future_thd_end.isoformat(),
        "tables": {},
    }
    for name, frame in tables.items():
        path = out / f"{name}.csv"
        frame.write_csv(path, date_format="%Y-%m-%d", null_value="")
        manifest["tables"][name] = {"rows": frame.height, "columns": frame.columns, "file": path.name}  # type: ignore[index]
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return out


def read_tables(in_dir: str | Path) -> dict[str, pl.DataFrame]:
    """Read the CSV outputs back with dates parsed, for validation and summaries."""
    inp = Path(in_dir)
    manifest = json.loads((inp / "_manifest.json").read_text(encoding="utf-8"))
    tables: dict[str, pl.DataFrame] = {}
    for name, meta in manifest["tables"].items():
        overrides: dict[str, pl.DataType] = {}
        for col in meta["columns"]:
            if col.endswith("_date"):
                overrides[col] = pl.Date
            elif col.startswith("is_"):
                overrides[col] = pl.Boolean
            elif col.endswith("_id") or col.endswith("_code") or col in ("currency",):
                overrides[col] = pl.Utf8
        tables[name] = pl.read_csv(inp / f"{name}.csv", schema_overrides=overrides, null_values=[""])
    return tables
