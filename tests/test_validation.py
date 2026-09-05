import polars as pl

from ta_exec_data_gen.validate import run_validations


def test_all_source_checks_pass(tables_medium, cfg_medium):
    results = run_validations(tables_medium, cfg_medium)
    failed = [r.name for r in results if not r.passed]
    assert not failed, failed
    assert len(results) > 60


def test_validation_detects_broken_identity(tables_medium, cfg_medium):
    broken = dict(tables_medium)
    snap = broken["ats_requisition_snapshot"]
    broken["ats_requisition_snapshot"] = snap.with_columns(
        openings_position=pl.when(pl.col("requisition_status") == "open")
        .then(pl.col("openings_position") + 1)
        .otherwise(pl.col("openings_position"))
    )
    results = run_validations(broken, cfg_medium)
    names = {r.name for r in results if not r.passed}
    assert any("requested = active fills + openings" in n for n in names)


def test_validation_detects_future_event(tables_medium, cfg_medium):
    broken = dict(tables_medium)
    hr = broken["hr_worker_event"]
    broken["hr_worker_event"] = hr.with_columns(
        event_date=pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(cfg_medium.dates.future_thd_end))
        .otherwise(pl.col("event_date"))
    )
    results = run_validations(broken, cfg_medium)
    assert any(("worker_event" in r.name and "as_of" in r.name and not r.passed) for r in results)
