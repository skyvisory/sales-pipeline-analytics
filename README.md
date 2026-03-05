# Sales Pipeline Analytics
**SQL + DuckDB + Streamlit — Interactive Revenue Dashboard**

![Python](https://img.shields.io/badge/Python-3.11-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-1.4.4-yellow)
![Streamlit](https://img.shields.io/badge/Streamlit-1.55-red)
![Plotly](https://img.shields.io/badge/Plotly-6.6-blue)

🔗 **[Live Dashboard](https://sales-pipeline-analytics-ukcnzhjsdzenfm8pb7vtt6.streamlit.app/)**

---

## The Business Problem

Sales teams fly blind without clear pipeline visibility.
This project builds an end-to-end analytics system answering
the questions every revenue leader asks weekly:

- Do we have enough pipeline to hit quota?
- Who is performing and who needs coaching?
- Where do deals stall?
- Which lead sources produce the best deals?

---

## Dashboard

**Five panels:**
- KPI cards — total revenue, active pipeline, win rate, sales cycle
- Pipeline funnel — stage conversion rates
- Quarterly revenue — closed revenue by quarter
- Rep leaderboard — quota attainment with traffic light colours
- Pipeline Health Score — composite metric per rep

---

## Key SQL Findings

| Finding | Detail |
|---|---|
| Win rate by segment | SMB 47% vs Enterprise 33% — smaller deals close easier |
| Sales cycle | 83-85 days consistent across all segments |
| Stage velocity | Deals stall longest in Negotiation at 85 days avg |
| Best lead source | Outbound 45.3% win rate — highest of all sources |
| Coverage | All reps pipeline constrained — active pipeline below required coverage |
| Top rep | David Kim 122.8% quota attainment |
| At risk | Lisa Park 68.7% — needs pipeline building |

---

## Pipeline Health Score

An invented composite metric combining three factors
into a single 0-100 score per rep:
```
Coverage Score  (40%) = actual coverage / required coverage
Win Rate Score  (35%) = close rate × 35
Velocity Score  (25%) = 90 days / avg cycle days

Required coverage = 1 / close_rate
→ personalised per rep not a blanket 3x rule
```

| Score | Status |
|---|---|
| > 80 | Strong |
| 60-80 | Healthy |
| 40-60 | At Risk |
| < 40 | Critical |

---

## RevOps Lessons Learned

- **Coverage ratio should use close rate not a blanket 3x rule**
  A rep closing 40% needs 2.5x pipeline. One closing 20% needs 5x.
  Personalised thresholds surface real risk that aggregate rules hide.

- **Quarterly revenue must use close date not created date**
  Created date measures pipeline activity.
  Close date measures revenue recognition.
  Conflating them misleads quarterly forecasts.

- **Outbound outperformed Referral on win rate (45% vs 39%)**
  Counterintuitive — referrals are assumed warmer leads.
  Suggests outbound targeting quality is higher than assumed,
  or referral deals face more internal scrutiny.

- **Negotiation is the longest active stage at 85 days**
  Deals stalling in Negotiation burn sales capacity.
  A 30-day SLA with mandatory manager review would
  force earlier disqualification decisions.

- **Pipeline health is multidimensional**
  Coverage alone misses win rate and velocity signals.
  A rep with 3x coverage but 20% win rate is in worse shape
  than one with 2x coverage and 45% win rate.

## Known Limitations

- **Pipeline funnel does not reflect sequential conversion**
  The synthetic data generator assigns final stages directly
  rather than simulating deals flowing through each stage.
  In a real CRM dataset the funnel would show decreasing
  volume from Qualified → Proposal → Negotiation → Closed Won.
  Here Closed Won (593) appears larger than Qualified (58)
  because most deals were assigned directly to closed stages.

- **Q4 2024 revenue spike is a data artifact**
  Deal close dates cluster at the dataset end date (Dec 2024)
  producing an artificially high Q4 2024 figure.
  In production this would be smoothed by continuous data flow.

- **All reps exceed quota**
  Quotas were calibrated after generation to produce realistic
  attainment variance (69-123%) but all reps remain above quota.
  A real dataset would show more distribution above and below.

- **Save rates not validated**
  Pipeline Health Score weights (40/35/25) are based on
  industry reasoning not empirical optimisation.
  A production version would derive weights from historical
  correlation between each metric and actual quota attainment.

---

## Project Structure
```
sales-pipeline-analytics/
│
├── scripts/
│   ├── generate_data.py      ← synthetic CRM data generator
│   ├── load_database.py      ← DuckDB loader
│   ├── run_queries.py        ← SQL query runner
│   └── startup.py            ← auto-generates data on deployment
│
├── sql/
│   └── 01_exploration.sql    ← 10 business queries
│
├── dashboard/
│   └── app.py                ← Streamlit dashboard
│
├── data/                     ← gitignored — generated locally
└── requirements.txt
```

---

## How to Run Locally
```bash
git clone https://github.com/skyvisory/sales-pipeline-analytics.git
cd sales-pipeline-analytics

pip install -r requirements.txt

python scripts/generate_data.py
python scripts/load_database.py

streamlit run dashboard/app.py
```

---

## Dataset

- **Source:** Synthetic — generated with Python Faker library
- **Records:** 2,000 opportunities
- **Period:** 2023-2024
- **Reps:** 8 across 4 regions (APAC, EMEA, AMER, LATAM)
- **Segments:** SMB, Mid-Market, Enterprise

---

*Built as Project 2 of a 5-project data analytics portfolio.*
*Previous: [Customer Churn Analysis](https://github.com/skyvisory/customer-churn-analysis)*