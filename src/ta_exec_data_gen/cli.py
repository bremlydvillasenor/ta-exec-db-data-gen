"""Command line interface.

ta-gen generate  --config config/default.yaml [--output data/raw] [--seed N]
ta-gen validate  --config config/default.yaml [--input data/raw]
ta-gen summary   --config config/default.yaml [--input data/raw]
ta-gen fixtures  --config config/default.yaml [--output data/fixtures/invalid]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from .config import load_config
from .fixtures import INVALID_CASES, write_invalid_fixtures
from .pipeline import generate
from .story import format_summary, summarise
from .validate import format_results, run_validations
from .writer import read_tables, write_manifest, write_tables


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config/default.yaml", help="YAML configuration file")
    parser.add_argument("--seed", type=int, default=None, help="override the configured random seed")
    parser.add_argument("-v", "--verbose", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ta-gen", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="generate the raw CSV sources, then validate them")
    _add_common(gen)
    gen.add_argument("--output", default=None, help="output directory (default: output.directory in config)")
    gen.add_argument("--skip-validation", action="store_true")

    val = sub.add_parser("validate", help="run source-level validation on an output directory")
    _add_common(val)
    val.add_argument("--input", default=None)

    summ = sub.add_parser("summary", help="print an indicative data-story summary of an output directory")
    _add_common(summ)
    summ.add_argument("--input", default=None)

    fix = sub.add_parser("fixtures", help="write the deliberately invalid extracts used to test validation")
    _add_common(fix)
    fix.add_argument("--output", default="data/fixtures/invalid")
    fix.add_argument("--positions", type=float, default=1.0, help="scaled-down monthly demand for the fixtures")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    overrides = {"seed": args.seed} if args.seed is not None else None
    cfg = load_config(args.config, overrides)
    log = logging.getLogger("ta_exec_data_gen")

    if args.command == "generate":
        out_dir = Path(args.output or cfg.output.directory)
        started = time.time()
        tables = generate(cfg)
        write_tables(tables, out_dir, cfg)
        log.info("wrote %d tables to %s in %.1fs", len(tables), out_dir, time.time() - started)
        for name, frame in tables.items():
            print(f"{name:28s} {frame.height:>9,d} rows")
        if args.skip_validation:
            write_manifest(tables, out_dir, cfg, validation={"status": "skipped"})
            return 0
        results = run_validations(tables, cfg)
        passed = all(r.passed for r in results)
        write_manifest(
            tables,
            out_dir,
            cfg,
            validation={
                "status": "passed" if passed else "failed",
                "checks": len(results),
                "failed_checks": [r.name for r in results if not r.passed],
            },
        )
        print(format_results(results))
        return 0 if passed else 1

    if args.command == "fixtures":
        out_dir = Path(args.output)
        written = write_invalid_fixtures(cfg, out_dir, base_positions=args.positions)
        for case, path in written.items():
            print(f"{case:34s} -> {path}")
        print(f"{len(INVALID_CASES)} invalid fixtures written to {out_dir}")
        return 0

    in_dir = Path(args.input or cfg.output.directory)
    tables = read_tables(in_dir)
    if args.command == "validate":
        results = run_validations(tables, cfg)
        print(format_results(results))
        for r in results:
            if not r.passed and r.sample is not None:
                print(f"\n--- sample failures: {r.name}\n{r.sample}")
        return 0 if all(r.passed for r in results) else 1
    if args.command == "summary":
        print(format_summary(summarise(tables, cfg)))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
