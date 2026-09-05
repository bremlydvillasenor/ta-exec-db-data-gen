"""Organisation reference extracts: business units, job families and job levels.

These are the source-side master data the dbt project turns into dim_business_unit,
dim_job_family and dim_job_level. Surrogate keys and the "Unknown" member are dbt's job.
"""

from __future__ import annotations

import polars as pl

from .config import GeneratorConfig


def build_business_units(cfg: GeneratorConfig) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "business_unit_code": [bu.code for bu in cfg.business_units],
            "business_unit_name": [bu.name for bu in cfg.business_units],
            "sort_order": [bu.sort_order for bu in cfg.business_units],
            "is_active": [True] * len(cfg.business_units),
        }
    ).sort("sort_order")


def build_job_families(cfg: GeneratorConfig) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "job_family_code": [jf.code for jf in cfg.job_families],
            "job_family_name": [jf.name for jf in cfg.job_families],
            "sort_order": [jf.sort_order for jf in cfg.job_families],
            "is_active": [True] * len(cfg.job_families),
        }
    ).sort("sort_order")


def build_job_levels(cfg: GeneratorConfig) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "job_level_code": [jl.code for jl in cfg.job_levels],
            "job_level_name": [jl.name for jl in cfg.job_levels],
            "level_rank": [jl.level_rank for jl in cfg.job_levels],
            "is_active": [True] * len(cfg.job_levels),
        }
    ).sort("level_rank")
