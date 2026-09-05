# The data story

The records are built so that the Executive Summary reads as one coherent picture:
**demand -> delivery -> speed -> risk -> pipeline -> quality -> expected outcome**. None
of the figures below are stored in the outputs. They are produced by `uv run ta-gen summary`,
which re-derives them from the raw files with simple rules to show what dbt and Power BI
will find. Figures are for the default seed (20260531).

## 1. Demand keeps growing and gets rushed in mid-2025

Positions by Target Hire Date grow about 1.5% a month from roughly 60-100 a month in
early 2024 to about 140-160 a month in 2026, with seasonal peaks in Q1 and dips in summer
and December. A **hiring surge** from May to August 2025 adds 30% demand and shortens
cycles: requisitions approved in that window run faster (median Time to Fill 33-35 days in
the July and August 2025 start cohorts against about 40 overall) with a looser bar.

Future demand is real planning data: 459 requisitions already approved on the as-of date
have a Target Hire Date after 31 May 2026 (through March 2027), 878 candidates are active
on them, and some already carry accepted offers waiting to start. No actual event on them is
dated after 31 May 2026.

## 2. Fill Rate differs by segment

| Segment | Fill Rate (THD on or before the as-of date) |
|---|---|
| Sales, Customer Success, Operations | 96-99% |
| General & Administrative, Product & Marketing | 92-95% |
| Engineering | 88% |
| Data & Analytics job family | 73% |

Sales and Operations hire in volume with multi-seat requisitions (686 of 3,006 requisitions
have more than one seat, up to 8), short cycles and strong pipelines. Data & Analytics and
Software Engineering carry most of the unfilled demand. For THD months January to May 2026
the Fill Rate sits between 80% and 91%, so the headline KPI is below the 90% target.

## 3. Speed: median Time to Fill about 40 days, with a long tail in technical roles

| Job family | Median Time to Fill (days) |
|---|---|
| Operations, Customer Success, Sales | 31-35 |
| Marketing, People, Finance | 44-51 |
| Software Engineering, IT & Security, Product Management | 66-78 |
| Data & Analytics | 97 |

## 4. TOAD risk concentrates where the constraints are

On the as-of date there are 498 open positions on 426 open requisitions:

| Risk band (days to TOAD) | Open positions |
|---|---|
| Missed (< 0) | 247 |
| High Risk (0-7) | 31 |
| Medium Risk (8-14) | 27 |
| On Track (>= 15) | 193 |

Data & Analytics (92 of 110 open seats at risk) and Software Engineering (75 of 118) carry
the exposure. Constraints follow the evidence: missed seats are mostly blamed on
`qualified_candidates` and `niche_skills`, seats with recent declines or reneges on
`compensation` or `candidate_availability`, and seats with candidates stuck late in the
process on `hiring_manager_delay` or `interview_capacity`. On-track seats are mostly
`no_material_constraint`.

Note that every open seat whose THD is on or before 31 May 2026 is necessarily "Missed",
because TOAD is always before THD. The High, Medium and On Track bands appear only when
the THD slicer includes months after the as-of date (see `design.md`).

## 5. The funnel bottleneck is the interview stage

| Stage | Active now | Historical conversion | Median completed days | SLA |
|---|---:|---:|---:|---:|
| Review | 380 | 52% | 3 | 3 |
| Screen | 254 | 65% | 4 | 4 |
| Assessment | 217 | 56% | 5 | 4 |
| Interview | 179 | 37% | 8 | 5 |
| Offer | 19 | 74% | 3 | 3 |

Interview conversion is lowest and slowest in Software Engineering (32%, 12 days), Data &
Analytics (33%, 14 days) and Product Management (32%, 13 days), which is what explains the
weaker delivery in those segments. Sales and Operations interviews convert at about 40% in
6 days. Offer conversion (74%) counts acceptance events; the 254 offers later lost to a
renege (187) or an employer rescind (67) remain successful conversions and remain in Time to
Fill, but their seats are back in open demand. 99 requisitions visibly return from `filled`
to `open` in the snapshot history because of this.

## 6. A budget freeze in Q4 2024

Requisitions exposed to the October-November 2024 freeze are cancelled far more often (27
first-cancelled requisitions in each of October and November 2024 against a median of 10 a
month), and accepted offers in that window are rescinded at six times the base rate
(rescinds double in November and December 2024). Pending accepts on a fully cancelled
requisition are rescinded; requisitions with someone already started lose only their open
seats (partial cancellation).

## 7. Quality: 60-Day Early Attrition around 8%, higher after the surge

Over the 12 fully matured start cohorts (April 2025 to March 2026) about 8-9% of started
hires leave within 60 days. The July and August 2025 cohorts, largely hired during the
surge, run at 13%, and November and December 2025 at 11-12%, while January 2026 is under
3%. The relationship with speed is visible but not forced: the fastest cohorts are the
surge cohorts, yet some slow cohorts also show high attrition (February 2025: 15% at a
41-day median). By business unit, Sales runs at about 14% and Engineering under 3%.

## 8. Source quirks dbt must handle

* 750 applications carry more than one accepted offer version of the same offer
  (administrative revisions); the earliest acceptance is the governed one.
* 4 applications carry two accepted offer cycles (ambiguous, to be quarantined). Both cycles
  end in a renege, so no seat depends on the resolution.
* 867 negotiation revisions (`superseded` versions) before acceptance.
* Duplicate HR rows: re-sent hire events and later-dated second termination rows.
* 492 requisitions have their THD and TOAD pushed out in later snapshots; the latest snapshot
  on or before the as-of date is the reporting state.
* 4,108 candidates applied to more than one requisition.
