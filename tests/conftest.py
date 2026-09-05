"""Shared fixtures: scaled-down configurations so the full pipeline runs in seconds."""

from __future__ import annotations

from pathlib import Path

import pytest

from ta_exec_data_gen.config import GeneratorConfig, load_config
from ta_exec_data_gen.pipeline import generate

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "default.yaml"


def scaled_config(base_positions: float, **extra) -> GeneratorConfig:
    overrides = {"demand": {"base_positions_per_month": base_positions}}
    overrides.update(extra)
    return load_config(DEFAULT_CONFIG, overrides)


@pytest.fixture(scope="session")
def cfg_small() -> GeneratorConfig:
    return scaled_config(8, offers={"quarantine_case_count": 2})


@pytest.fixture(scope="session")
def tables_small(cfg_small):
    return generate(cfg_small)


@pytest.fixture(scope="session")
def cfg_medium() -> GeneratorConfig:
    return scaled_config(30)


@pytest.fixture(scope="session")
def tables_medium(cfg_medium):
    return generate(cfg_medium)
