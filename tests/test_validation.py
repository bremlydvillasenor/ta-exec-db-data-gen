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


def test_a_missing_table_or_column_is_reported_instead_of_crashing(tables_medium, cfg_medium):
    """The shape check is a precondition, so nothing may touch the data before it runs.

    A missing file used to raise KeyError and a missing key column raised deep inside a
    Polars expression - both real inputs, neither one producing a finding a reviewer could
    read.
    """
    missing_table = {k: v for k, v in tables_medium.items() if k != "ats_offer"}
    failed = [r.name for r in run_validations(missing_table, cfg_medium) if not r.passed]
    assert failed == ["ats_offer: table is present in the extract"]

    for table, column in (
        ("ats_requisition_snapshot", "snapshot_date"),
        ("ats_requisition_snapshot", "requisition_id"),
        ("ats_application", "application_id"),
        ("ats_offer", "offer_accepted_date"),
    ):
        dropped = dict(tables_medium)
        dropped[table] = dropped[table].drop(column)
        failed = [r.name for r in run_validations(dropped, cfg_medium) if not r.passed]
        assert failed == [f"{table}: declared columns are present"], f"{table}.{column}: {failed}"

    undeclared = dict(tables_medium)
    undeclared["ats_interview"] = tables_medium["ats_application"].head(1)
    failed = [r.name for r in run_validations(undeclared, cfg_medium) if not r.passed]
    assert failed == ["extract: no undeclared tables"]


def test_a_repeat_attempt_between_acceptance_and_loss_fails(tables_medium, cfg_medium):
    """An accepted offer ends at its rescind or renege, not when the Offer stage closed.

    The Offer stage exits on acceptance, so a stage-based attempt end made the weeks between
    acceptance and a later loss look free - and a second application submitted inside that
    window passed every check.
    """
    app, off, stg = (
        tables_medium["ats_application"],
        tables_medium["ats_offer"],
        tables_medium["ats_stage_history"],
    )
    lost = (
        off.filter(
            pl.col("offer_accepted_date").is_not_null()
            & (pl.col("offer_rescinded_date").is_not_null() | pl.col("candidate_renege_date").is_not_null())
        )
        .select(
            "application_id",
            "requisition_id",
            loss_date=pl.coalesce("offer_rescinded_date", "candidate_renege_date"),
        )
        .join(
            stg.group_by("application_id").agg(last_stage_exit=pl.col("stage_exit_date").max()),
            on="application_id",
            how="left",
        )
        .join(app.select("application_id", "candidate_id"), on="application_id")
    )
    # a second application on the same requisition, submitted after the first attempt's last
    # stage closed but before the loss that actually ended it
    overlap = (
        lost.join(
            app.select(
                second_id="application_id",
                requisition_id="requisition_id",
                second_candidate="candidate_id",
                second_date="application_date",
            ),
            on="requisition_id",
        )
        .filter(
            (pl.col("second_candidate") != pl.col("candidate_id"))
            & (pl.col("second_date") > pl.col("last_stage_exit"))
            & (pl.col("second_date") < pl.col("loss_date"))
        )
        .sort(["application_id", "second_id"])
    )
    assert overlap.height, "no acceptance-to-loss window to place a second attempt in"
    case = overlap.row(0, named=True)

    broken = dict(tables_medium)
    broken["ats_application"] = app.with_columns(
        candidate_id=pl.when(pl.col("application_id") == case["second_id"])
        .then(pl.lit(case["candidate_id"]))
        .otherwise(pl.col("candidate_id"))
    )
    failed = [r.name for r in run_validations(broken, cfg_medium) if not r.passed]
    assert "application: a repeated attempt starts after the previous one ended" in failed


def test_an_attempt_still_holding_an_acceptance_never_ends(tables_medium, cfg_medium):
    """A candidate who took the seat cannot try again for it, however the offer stage looks."""
    app, off = tables_medium["ats_application"], tables_medium["ats_offer"]
    live = off.filter(
        pl.col("offer_accepted_date").is_not_null()
        & pl.col("offer_rescinded_date").is_null()
        & pl.col("candidate_renege_date").is_null()
    ).join(app.select("application_id", "candidate_id", "requisition_id"), on="application_id")
    # a multi-seat requisition still takes applications after one seat is accepted, so hand
    # one of those later applications to the person who already holds the seat
    pairs = (
        live.join(
            app.select(
                second_id="application_id",
                requisition_id="requisition_id",
                second_candidate="candidate_id",
                second_date="application_date",
            ),
            on="requisition_id",
        )
        .filter(
            (pl.col("second_candidate") != pl.col("candidate_id"))
            & (pl.col("second_date") > pl.col("offer_accepted_date"))
        )
        .sort(["application_id", "second_id"])
    )
    assert pairs.height, "no application submitted after a live acceptance on the same requisition"
    seat = pairs.row(0, named=True)

    broken = dict(tables_medium)
    broken["ats_application"] = app.with_columns(
        candidate_id=pl.when(pl.col("application_id") == seat["second_id"])
        .then(pl.lit(seat["candidate_id"]))
        .otherwise(pl.col("candidate_id"))
    )
    failed = [r.name for r in run_validations(broken, cfg_medium) if not r.passed]
    assert "application: a repeated attempt starts after the previous one ended" in failed
