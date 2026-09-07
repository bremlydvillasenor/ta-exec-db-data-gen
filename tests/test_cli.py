import json

import yaml

from ta_exec_data_gen.cli import main
from ta_exec_data_gen.fixtures import INVALID_CASES

from .conftest import DEFAULT_CONFIG


def test_generate_validate_summary_round_trip(tmp_path):
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text())
    raw["demand"]["base_positions_per_month"] = 8
    raw["output"]["directory"] = str(tmp_path / "out")
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(yaml.safe_dump(raw))

    assert main(["generate", "--config", str(cfg_path)]) == 0
    manifest = json.loads((tmp_path / "out" / "manifest.json").read_text())
    assert manifest["as_of_date"] == "2026-05-31"
    # the handoff facts the contract asks a manifest to carry
    assert manifest["contract"]["release"] == "1.3"
    assert len(manifest["contract"]["commit"]) == 40
    assert manifest["extracted_at"] == "2026-05-31T23:59:59Z"
    assert manifest["updated_at_available"] is True
    assert manifest["validation"]["status"] == "passed"
    assert manifest["validation"]["failed_checks"] == []
    assert manifest["effective_configuration"]["seed"] == manifest["seed"]
    for name, meta in manifest["tables"].items():
        path = tmp_path / "out" / meta["file"]
        assert path.exists(), name
        assert sum(1 for _ in path.open()) - 1 == meta["rows"]
        assert len(meta["sha256"]) == 64
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


def test_fixtures_command_writes_one_directory_per_invalid_case(tmp_path):
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text())
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(yaml.safe_dump(raw))
    out = tmp_path / "invalid"
    assert main(["fixtures", "--config", str(cfg_path), "--output", str(out), "--positions", "6"]) == 0
    assert (out / "README.md").exists()
    for case in INVALID_CASES:
        assert (out / case / "manifest.json").exists(), case
