import datetime as dt

import pytest
from pydantic import ValidationError

from ta_exec_data_gen.config import load_config

from .conftest import DEFAULT_CONFIG


def test_default_config_loads():
    cfg = load_config(DEFAULT_CONFIG)
    assert cfg.dates.as_of == dt.date(2026, 5, 31)
    assert cfg.dates.history_start == dt.date(2024, 1, 1)
    assert cfg.dates.future_thd_end == dt.date(2027, 5, 31)
    assert cfg.funnel.stages == ["review", "screen", "assessment", "interview", "offer"]
    assert "no_material_constraint" in cfg.hiring_constraints


def test_overrides_are_deep_merged():
    cfg = load_config(DEFAULT_CONFIG, {"demand": {"base_positions_per_month": 5}, "seed": 7})
    assert cfg.demand.base_positions_per_month == 5
    assert cfg.seed == 7
    assert cfg.demand.monthly_growth_rate == load_config(DEFAULT_CONFIG).demand.monthly_growth_rate


def test_unknown_job_family_in_mix_is_rejected():
    with pytest.raises(ValidationError):
        load_config(DEFAULT_CONFIG, {"business_unit_job_family_mix": {"ENG": {"XXX": 1.0}}})


def test_date_order_is_enforced():
    with pytest.raises(ValidationError):
        load_config(DEFAULT_CONFIG, {"dates": {"as_of": "2028-01-01"}})


def test_unknown_keys_are_rejected():
    with pytest.raises(ValidationError):
        load_config(DEFAULT_CONFIG, {"demand": {"not_a_setting": 1}})
