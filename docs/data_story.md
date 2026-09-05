# The data story

The records are built so that the Executive Summary reads as one coherent picture:
**demand -> delivery -> speed -> risk -> pipeline -> forecast -> quality**. None of the
figures below are stored in the outputs. They are produced by `uv run ta-gen summary`,
which re-derives them from the raw files with simple rules to show what dbt and Power BI
will find. Figures are for the default seed (20260531).

Where the contract states a population rule, the summary follows it, so these numbers can
be compared with the governed marts instead of quietly differing from them: quarantined
applications are held out of every figure, including the funnel stage counts and the
forecast yield, Time to Fill runs on non-cancelled requisitions, and the forecast uses the
FCST-01..04 definitions including the segment fallback and the requisition-level cap.

## 1. Demand keeps growing and gets rushed in mid-2025

Positions by Target Hire Date grow about 1.5% a month, from roughly 40-110 a month in 2024
(January and February are thin because no requisition can be approved before 1 January
2024) to 134-159 a month in early 2026, with seasonal peaks in Q1 and dips in summer and
December. A **hiring surge** from May to August 2025 adds 30% demand and shortens cycles:
requisitions approved in that window run faster (median Time to Fill 30-37 days in the July
to September 2025 start cohorts against about 40 overall) with a looser bar.

Future demand is real planning data: 481 requisitions already approved on the as-of date
have a Target Hire Date after 31 May 2026 (678 planned positions, running all the way to
31 May 2027), 865 candidates are active on them, and some already carry accepted offers
waiting to start. No actual event on them is dated after 31 May 2026. The far end of the
plan is thin by design - a THD in May 2027 only exists if the requisition was already
approved on the as-of date, which means an annual-plan approval lead of about 12 months.

## 2. Fill Rate differs by segment

| Segment | Fill Rate (THD on or before the as-of date) |
|---|---|
| Sales, Customer Success, Marketing, People, Operations | 98-100% |
| Finance, IT & Security | 93-94% |
| Software Engineering | 90% |
| Product Management | 83% |
| Data & Analytics | 70% |

By business unit: Sales 99%, Customer Success 98%, Operations 95%, General &
Administrative 96%, Product & Marketing 92%, Engineering 88%.

Sales and Operations hire in volume with multi-seat requisitions (649 of 3,024
requisitions have more than one seat, up to 8), short cycles and strong pipelines. Data &
Analytics and Software Engineering carry most of the unfilled demand. For THD months
January to May 2026 the Fill Rate sits between 80% and 93% (86% for the period as a
whole), so the headline KPI is below the 90% target.

## 3. Speed: median Time to Fill about 40 days, with a long tail in technical roles

| Job family | Median Time to Fill (days) |
|---|---|
| Operations, Customer Success, Sales | 32-35 |
| Marketing, People | 43-44 |
| Finance | 60 |
| Software Engineering, IT & Security, Product Management | 68-83 |
| Data & Analytics | 86 |

The population narrows from the source in two contract-driven steps, both shown by
`time_to_fill_population` in the summary:

| Step | Applications |
|---|---:|
| Applications with an acceptance in the source | 3,718 |
| less quarantined (more than one acceptance cycle) | -4 |
| less acceptances on cancelled requisitions | -33 |
| **Governed EXEC-05 population** | **3,681** |

Median Time to Fill is 40 days on both ends of that table, but the counts differ, and it
is the count that reconciles with SUPP-06. Offers later rescinded or reneged stay in the
population: Time to Fill measures the recruiting cycle TA actually completed.

## 4. TOAD risk concentrates where the constraints are

On the as-of date there are 508 open positions on 433 open requisitions:

| Risk band (days to TOAD) | Open positions |
|---|---|
| Missed (< 0) | 248 |
| High Risk (0-7) | 32 |
| Medium Risk (8-14) | 26 |
| On Track (>= 15) | 202 |

Data & Analytics (88 of 113 open seats at risk) and Software Engineering (83 of 126) carry
the exposure. Constraints follow the evidence: missed seats are mostly blamed on
`qualified_candidates` (80) and `niche_skills` (69), seats with recent declines or reneges
on `compensation` or `candidate_availability`, and seats with candidates stuck late in the
process on `hiring_manager_delay` or `interview_capacity`. On-track seats are mostly
`no_material_constraint` (157 of 202).

**The risk visual depends on the THD selection, and one common selection makes it
degenerate.** TOAD is always on or before THD, so every open seat whose THD is on or before
31 May 2026 has already missed its TOAD. The other three bands exist only when the THD
slicer reaches past the as-of date:

| THD selection | Missed | High | Medium | On Track | Open | At risk |
|---|---:|---:|---:|---:|---:|---:|
| Jan-May 2026 (wireframe default) | 102 | 0 | 0 | 0 | 102 | 100% |
| Jun 2026 - May 2027 (future THD) | 81 | 32 | 26 | 202 | 341 | 40.8% |
| All THD (no period filter) | 248 | 32 | 26 | 202 | 508 | 60.2% |

This is a property of the metric definition, not of the data. See `design.md` for the
open question it raises for the contract.

## 5. The funnel bottleneck is the interview stage

| Stage | Active now | Historical conversion | Median completed days | SLA |
|---|---:|---:|---:|---:|
| Review | 367 | 52% | 3 | 3 |
| Screen | 252 | 65% | 4 | 4 |
| Assessment | 203 | 56% | 5 | 4 |
| Interview | 192 | 37% | 8 | 5 |
| Offer | 18 | 74% | 3 | 3 |

Interview conversion is lowest and slowest in Software Engineering (32%, 12 days), Data &
Analytics (31%, 14 days) and Product Management (30%, 13 days), which is what explains the
weaker delivery in those segments. Sales and Operations interviews convert at about 38-42%
in 6 days. Offer conversion (74%) counts acceptance events; the 238 offers later lost to a
renege (166) or an employer rescind (72) remain successful conversions and remain in Time
to Fill, but their seats are back in open demand. 88 requisitions visibly return from
`filled` to `open` in the snapshot history because of this.

Stage counts here exclude the four quarantined applications, which removes 4 completed
events from every stage. The offer stage is the one place where this changes a ratio rather
than only a count: its numerator is the governed acceptance count, so its denominator has to
be the governed offer population as well.

## 6. Forecast: the active pipeline closes about a quarter of the shortfall

The forecast trains a stage-to-active-fill yield per segment, applies it to the candidates
who are active on the as-of date, and caps each requisition at its remaining openings.

| THD selection | Requested | Filled | Expected pipeline fills | Fill Rate | Forecast Fill Rate |
|---|---:|---:|---:|---:|---:|
| All THD | 3,988 | 3,480 | 119.3 | 87.3% | 90.3% |
| Jan-May 2026 | 725 | 623 | 11.0 | 85.9% | 87.5% |
| THD on or before as-of | 3,310 | 3,143 | 18.8 | 95.0% | 95.5% |
| THD after as-of | 678 | 337 | 100.4 | 49.7% | 64.5% |

Expected fills recover 119 of the 508 unfilled positions, about 23%. The lift sits
where it should: nearly all of it belongs to demand whose Target Hire Date has
not arrived yet, because that is where the live pipeline is. Delivered months gain almost
nothing, which is the honest answer - a missed February 2026 seat is not rescued by a
forecast. Across the whole plan the forecast just crosses the 90% target while the actual
Fill Rate does not, which is the tension the page is meant to show.

Yields behave as a recruiting funnel should: median yield rises from about 4% at review to
about 70% at the offer stage, and it varies by segment rather than being one global number.
Small segments fall back through `bu_jf_jl -> jf_jl -> jf -> all` at 30 observations, so no
requisition is forecast from a handful of rows.

## 7. A budget freeze in Q4 2024

Requisitions exposed to the October-November 2024 freeze are cancelled far more often, and
accepted offers in that window are rescinded at six times the base rate. Pending accepts on
a fully cancelled requisition are rescinded; requisitions with someone already started lose
only their open seats (partial cancellation).

## 8. Quality: 60-Day Early Attrition around 9%, higher after the surge

Over the 12 fully matured start cohorts (April 2025 to March 2026), 146 of 1,583 started
hires left within 60 days - **9.2%**, above the 8% target. The July 2025 cohort, largely
hired during the surge, runs at 15% and September 2025 at 13%, while January 2026 is at 4%.
The relationship with speed is visible but not forced: the fastest cohorts are the surge
cohorts, yet some slow cohorts also show high attrition (February 2025: 11% at a 39-day
median). By business unit, Sales runs at about 12% and Engineering under 5%.

## 9. Source quirks dbt must handle

* 733 applications carry more than one accepted offer version of the same offer
  (administrative revisions); the earliest acceptance is the governed one.
* 4 applications carry two accepted offer cycles (ambiguous, to be quarantined). Both cycles
  end in a renege, so no seat depends on the resolution, but they must still be recorded in
  the audit model and kept out of the application fact.
* 852 negotiation revisions (`superseded` versions) before acceptance.
* Duplicate HR rows: re-sent hire events and later-dated second termination rows.
* 496 requisitions have their THD and TOAD pushed out in later snapshots; the latest
  snapshot on or before the as-of date is the reporting state.
* 3,895 candidates applied to more than one requisition. No candidate holds two live
  acceptances, is hired twice, keeps an application active elsewhere after taking a seat,
  or applies again once they have accepted one.

## 10. Reconciling with the wireframe

The wireframe in `ta-exec-db` carries illustrative numbers, drawn before this generator
existed. They are not produced by any seed of this configuration and should be replaced
with generated figures when the dashboard is built. For its Jan-May 2026 THD selection:

| Measure | Wireframe | Generated |
|---|---:|---:|
| Requested positions (demand) | 1,126 | 725 |
| Filled positions | 953 | 623 |
| Open positions | 173 | 102 |
| Fill Rate | 84.6% | 85.9% |
| Open positions by risk band | 22 / 19 / 20 / 112 | 102 / 0 / 0 / 0 |
| 60-Day Early Attrition | 8.2% (87 of 1,064) | 9.2% (146 of 1,583) |

The rates are close; the volumes are not, because the wireframe assumed a larger hiring
programme. The risk band row is the one difference that no amount of retuning fixes - see
section 4 and `design.md`.
