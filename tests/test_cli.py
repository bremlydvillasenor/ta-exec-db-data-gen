import json

import yaml

from ta_exec_data_gen.cli import main

from .conftest import DEFAULT_CONFIG


def test_generate_validate_summary_round_trip(tmp_path):
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text())
    raw["demand"]["base_positions_per_month"] = 8
    raw["output"]["directory"] = str(tmp_path / "out")
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(yaml.safe_dump(raw))

    assert main(["generate", "--config", str(cfg_path)]) == 0
    manifest = json.loads((tmp_path / "out" / "_manifest.json").read_text())
    assert manifest["as_of_date"] == "2026-05-31"
    for name, meta in manifest["tables"].items():
        path = tmp_path / "out" / meta["file"]
        assert path.exists(), name
        assert sum(1 for _ in path.open()) - 1 == meta["rows"]
    assert main(["validate", "--config", str(cfg_path)]) == 0
    assert main(["summary", "--config", str(cfg_path)]) == 0


def test_seed_override_changes_fingerprint_free_output(tmp_path):
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text())
    raw["demand"]["base_positions_per_month"] = 6
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(yaml.safe_dump(raw))
    assert (
        main(
            ["generate", "--config", str(cfg_path), "--output", str(tmp_path / "a"), "--seed", "1", "--skip-validation"]
        )
        == 0
    )
    assert (
        main(
            ["generate", "--config", str(cfg_path), "--output", str(tmp_path / "b"), "--seed", "2", "--skip-validation"]
        )
        == 0
    )
    a = (tmp_path / "a" / "ats_application.csv").read_text()
    b = (tmp_path / "b" / "ats_application.csv").read_text()
    assert a != b
