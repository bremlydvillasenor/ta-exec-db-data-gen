from ta_exec_data_gen.pipeline import OUTPUT_TABLES, generate

from .conftest import scaled_config


def test_same_seed_same_output(cfg_small, tables_small):
    again = generate(cfg_small)
    assert list(again) == OUTPUT_TABLES
    for name in OUTPUT_TABLES:
        assert tables_small[name].equals(again[name]), name


def test_different_seed_changes_output(cfg_small, tables_small):
    other = generate(scaled_config(8, seed=cfg_small.seed + 1, offers={"quarantine_case_count": 2}))
    assert not tables_small["ats_application"].equals(other["ats_application"])
