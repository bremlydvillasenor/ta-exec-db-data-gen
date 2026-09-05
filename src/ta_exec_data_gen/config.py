"""Typed configuration for the generator, loaded from YAML and validated with pydantic."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DatesConfig(StrictModel):
    history_start: dt.date
    as_of: dt.date
    future_thd_end: dt.date

    @model_validator(mode="after")
    def _ordered(self) -> DatesConfig:
        if not self.history_start < self.as_of < self.future_thd_end:
            raise ValueError("dates must satisfy history_start < as_of < future_thd_end")
        return self


class OutputConfig(StrictModel):
    directory: str = "data/raw"


class TriangularDays(StrictModel):
    min: int
    mode: int
    max: int

    @model_validator(mode="after")
    def _ordered(self) -> TriangularDays:
        if not self.min <= self.mode <= self.max:
            raise ValueError("triangular range must satisfy min <= mode <= max")
        return self


class DemandConfig(StrictModel):
    base_positions_per_month: float = Field(gt=0)
    monthly_growth_rate: float = Field(ge=-0.5, le=0.5)
    seasonality: list[float] = Field(min_length=12, max_length=12)
    approval_lead_days: TriangularDays
    early_plan_share: float = Field(ge=0, le=1)
    early_plan_lead_days: TriangularDays
    min_thd_gap_days: int = Field(ge=1)


class SurgeEpisode(StrictModel):
    start: dt.date
    end: dt.date
    demand_multiplier: float = 1.0
    cycle_time_multiplier: float = 1.0
    pass_rate_multiplier: float = 1.0
    early_attrition_multiplier: float = 1.0


class FreezeEpisode(StrictModel):
    start: dt.date
    end: dt.date
    cancellation_probability: float = Field(ge=0, le=1)
    rescind_multiplier: float = Field(ge=0)


class EpisodesConfig(StrictModel):
    hiring_surge: SurgeEpisode
    hiring_freeze: FreezeEpisode


class RequisitionsConfig(StrictModel):
    base_cancellation_probability: float = Field(ge=0, le=1)
    cancellation_day_range: tuple[int, int]
    stale_cancel_probability: float = Field(ge=0, le=1)
    stale_cancel_days_after_thd: tuple[int, int]
    partial_cancel_probability: float = Field(ge=0, le=1)
    partial_cancel_day_range: tuple[int, int]
    rebaseline_probability: float = Field(ge=0, le=1)
    rebaseline_delay_days: tuple[int, int]
    rebaseline_shift_days: tuple[int, int]
    second_rebaseline_probability: float = Field(ge=0, le=1)
    snapshot_keep_days_after_close: int = Field(ge=0)
    recruiter_count: int = Field(ge=1)
    hiring_manager_count: int = Field(ge=1)
    locations: list[str] = Field(min_length=1)


class DispositionReasons(StrictModel):
    rejected: list[str] = Field(min_length=1)
    withdrawn: list[str] = Field(min_length=1)


class FunnelConfig(StrictModel):
    stages: list[str] = Field(min_length=5, max_length=5)
    stage_sla_days: list[int] = Field(min_length=5, max_length=5)
    duration_sigma: float = Field(gt=0)
    burst_window_days: int = Field(ge=1)
    trickle_rate_share: float = Field(ge=0)
    reopen_burst_share: float = Field(ge=0)
    max_applications_per_requisition: int = Field(ge=1)
    pipeline_cut_lag_days: tuple[int, int]
    cut_withdrawn_share: float = Field(ge=0, le=1)
    withdrawn_share_of_exits: float = Field(ge=0, le=1)
    offer_withdrawn_share: float = Field(ge=0, le=1)
    candidate_pool_reuse: float = Field(ge=0, le=1)
    disposition_reasons: DispositionReasons


class OffersConfig(StrictModel):
    negotiation_revision_probability: float = Field(ge=0, le=1)
    admin_revision_probability: float = Field(ge=0, le=1)
    admin_revision_reasons: list[str] = Field(min_length=1)
    start_date_revision_days: tuple[int, int]
    quarantine_case_count: int = Field(ge=0)
    base_rescind_probability: float = Field(ge=0, le=1)
    currency: str = "USD"


class TerminationReasons(StrictModel):
    early: list[str] = Field(min_length=1)
    late: list[str] = Field(min_length=1)


class HrConfig(StrictModel):
    duplicate_hire_event_share: float = Field(ge=0, le=1)
    duplicate_termination_share: float = Field(ge=0, le=1)
    duplicate_termination_shift_days: tuple[int, int]
    late_attrition_annual_rate: float = Field(ge=0, le=1)
    early_tenure_days: tuple[int, int]
    termination_reasons: TerminationReasons


class BusinessUnit(StrictModel):
    code: str
    name: str
    weight: float = Field(gt=0)
    sort_order: int


class JobLevel(StrictModel):
    code: str
    name: str
    level_rank: int
    weight: float = Field(gt=0)
    duration_multiplier: float = Field(gt=0)
    interview_pass_multiplier: float = Field(gt=0)
    toad_lead_days: int = Field(ge=0)
    notice_days: int = Field(ge=0)
    attrition_multiplier: float = Field(gt=0)


class JobFamily(StrictModel):
    code: str
    name: str
    role_title: str
    sort_order: int
    apps_per_position: float = Field(gt=0)
    stage_pass: list[float] = Field(min_length=4, max_length=4)
    offer_accept_rate: float = Field(gt=0, le=1)
    stage_duration_median_days: list[float] = Field(min_length=5, max_length=5)
    multi_position_probability: float = Field(ge=0, le=1)
    max_positions: int = Field(ge=1)
    renege_rate: float = Field(ge=0, le=1)
    early_attrition_rate: float = Field(ge=0, le=1)
    niche_share: float = Field(ge=0, le=1)
    constraint_weights: dict[str, float]


class StoryConfig(StrictModel):
    """Governed reference values mirrored from ta-exec-db, used only by the story summary."""

    forecast_min_segment_observations: int = Field(default=30, ge=1)
    fill_rate_target: float = Field(default=0.90, ge=0, le=1)
    risk_high_max_days: int = Field(default=7, ge=0)
    risk_medium_max_days: int = Field(default=14, ge=0)

    @model_validator(mode="after")
    def _ordered(self) -> StoryConfig:
        if self.risk_high_max_days >= self.risk_medium_max_days:
            raise ValueError("risk_high_max_days must be below risk_medium_max_days")
        return self


class GeneratorConfig(StrictModel):
    seed: int
    dates: DatesConfig
    output: OutputConfig = OutputConfig()
    demand: DemandConfig
    episodes: EpisodesConfig
    requisitions: RequisitionsConfig
    funnel: FunnelConfig
    offers: OffersConfig
    hr: HrConfig
    story: StoryConfig = StoryConfig()
    business_units: list[BusinessUnit] = Field(min_length=1)
    business_unit_job_family_mix: dict[str, dict[str, float]]
    job_levels: list[JobLevel] = Field(min_length=1)
    job_families: list[JobFamily] = Field(min_length=1)
    hiring_constraints: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def _cross_checks(self) -> GeneratorConfig:
        jf_codes = {jf.code for jf in self.job_families}
        bu_codes = {bu.code for bu in self.business_units}
        if len(jf_codes) != len(self.job_families):
            raise ValueError("job family codes must be unique")
        if len(bu_codes) != len(self.business_units):
            raise ValueError("business unit codes must be unique")
        for bu in self.business_units:
            mix = self.business_unit_job_family_mix.get(bu.code)
            if not mix:
                raise ValueError(f"business unit {bu.code} has no job family mix")
            unknown = set(mix) - jf_codes
            if unknown:
                raise ValueError(f"business unit {bu.code} mixes unknown job families {unknown}")
        constraint_codes = set(self.hiring_constraints)
        if "no_material_constraint" not in constraint_codes:
            raise ValueError("hiring_constraints must include no_material_constraint")
        for jf in self.job_families:
            unknown = set(jf.constraint_weights) - constraint_codes
            if unknown:
                raise ValueError(f"job family {jf.code} uses unknown constraints {unknown}")
        ranks = [jl.level_rank for jl in self.job_levels]
        if len(set(ranks)) != len(ranks):
            raise ValueError("job level ranks must be unique")
        return self

    # convenience lookups -------------------------------------------------
    def job_family(self, code: str) -> JobFamily:
        return next(jf for jf in self.job_families if jf.code == code)

    def job_level(self, code: str) -> JobLevel:
        return next(jl for jl in self.job_levels if jl.code == code)


def load_config(path: str | Path, overrides: dict[str, Any] | None = None) -> GeneratorConfig:
    """Load and validate a YAML configuration file, with optional top-level overrides."""
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if overrides:
        raw = _deep_merge(raw, overrides)
    return GeneratorConfig.model_validate(raw)


def _deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out
