import polars as pl

from ta_exec_data_gen.fixtures import INVALID_CASES, build_invalid_fixtures
from ta_exec_data_gen.validate import SCHEMA, run_validations


def test_all_source_checks_pass(tables_medium, cfg_medium):
    results = run_validations(tables_medium, cfg_medium)
    failed = [r.name for r in results if not r.passed]
    assert not failed, failed
    assert len(results) > 100


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


def test_every_invalid_fixture_trips_its_own_check(tables_small, cfg_small):
    """The deliberately invalid extracts must fail, and fail for the documented reason."""
    for name, broken in build_invalid_fixtures(cfg_small, tables_small).items():
        expected = INVALID_CASES[name][1]
        failed = [r.name for r in run_validations(broken, cfg_small) if not r.passed]
        assert failed, f"{name} produced no validation failure"
        assert any(expected in n for n in failed), f"{name}: expected a check containing {expected!r}, got {failed}"


def test_a_missing_value_in_any_required_column_fails(tables_medium, cfg_medium):
    """Null one required column at a time; every one of them must be caught.

    Comparison and uniqueness rules cannot see this: a comparison against null evaluates to
    null, so an empty identifier, status, date or quantity would otherwise pass every
    downstream check untouched.
    """
    for table, columns in SCHEMA.items():
        for column, (_, required) in columns.items():
            if not required:
                continue
            broken = dict(tables_medium)
            frame = broken[table]
            broken[table] = frame.with_columns(
                pl.when(pl.int_range(pl.len()) == 0).then(None).otherwise(pl.col(column)).alias(column)
            )
            failed = [r.name for r in run_validations(broken, cfg_medium) if not r.passed]
            assert f"{table}: required columns have no missing values" in failed, f"{table}.{column} passed"


def test_a_wrong_data_type_or_shape_fails_instead_of_crashing(tables_medium, cfg_medium):
    """The declared shape is a precondition: report it and stop, never raise mid-run."""
    retyped = dict(tables_medium)
    retyped["ats_stage_history"] = retyped["ats_stage_history"].with_columns(
        stage_sequence_number=pl.col("stage_sequence_number").cast(pl.Utf8)
    )
    failed = [r.name for r in run_validations(retyped, cfg_medium) if not r.passed]
    assert failed == ["ats_stage_history: columns have the declared data type"]

    dropped = dict(tables_medium)
    dropped["ats_offer"] = dropped["ats_offer"].drop("planned_start_date")
    assert "ats_offer: declared columns are present" in [
        r.name for r in run_validations(dropped, cfg_medium) if not r.passed
    ]

    extra = dict(tables_medium)
    extra["ats_offer"] = extra["ats_offer"].with_columns(is_active_fill=pl.lit(True))
    assert "ats_offer: no undeclared columns" in [
        r.name for r in run_validations(extra, cfg_medium) if not r.passed
    ]


def test_updated_at_must_reflect_the_latest_recorded_event(tables_medium, cfg_medium):
    """A timestamp frozen at the opening event would make an incremental load miss the change."""
    cases = {
        "ats_stage_history": ("stage_entry_date", "stage_history: updated_at reflects its latest recorded date"),
        "ats_offer": ("offer_extended_date", "offer: updated_at reflects its latest recorded date"),
        "ats_application": ("application_date", "application: updated_at reflects its latest recorded date"),
    }
    for table, (column, expected) in cases.items():
        broken = dict(tables_medium)
        broken[table] = broken[table].with_columns(updated_at=pl.col(column).cast(pl.Datetime("us")))
        failed = [r.name for r in run_validations(broken, cfg_medium) if not r.passed]
        assert expected in failed, f"{table}: a stale updated_at passed validation"
