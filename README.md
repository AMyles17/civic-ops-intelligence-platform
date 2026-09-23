# Civic Operations Intelligence Platform

An end-to-end data platform for NYC 311 service request data — anomaly detection, trend analysis, an interactive executive dashboard, and a natural language query interface powered by an LLM agent with a working evaluation harness.

Built as a second full-stack AI/data project, deliberately outside my first project's domain (revenue analytics), to demonstrate the same build pattern applied to a different kind of problem: public-sector operational data instead of business metrics.

**[View the live, interactive dashboard →](https://datastudio.google.com/reporting/09b4083f-58b3-4744-bb7c-e244ce08405a)**

---

## What it does

- **Live ingestion pipeline** — pulls NYC 311 service request data directly from NYC Open Data's public API into BigQuery, not a static one-time snapshot
- **Anomaly detection** — flags unusual daily request volumes using a day-of-week-aware statistical model, validated against real, confirmed events in the data
- **Trend analysis** — category and borough breakdowns over time via SQL views
- **Executive dashboard** — an interactive Looker Studio dashboard with filterable views (daily trend, category breakdown, borough breakdown)
- **Natural language interface** — ask plain-English questions about the data (e.g. *"Which borough had the most heating complaints?"*) and get an accurate, data-grounded answer, powered by LangChain + Gemini
- **Evaluation harness** — a repeatable, automated test suite that checks the NL interface's answers against known-correct values, not just "did it run"

---

## Stack

| Layer | Tool |
|---|---|
| Data warehouse | Google BigQuery |
| Data source | [NYC Open Data — 311 Service Requests](https://data.cityofnewyork.us/resource/erm2-nwe9.json) (live Socrata API) |
| Analysis | Python, pandas, SQL (Google Colab) |
| Dashboard | Looker Studio |
| NL interface | LangChain + Google Gemini API |
| Eval harness | Custom Python test runner |

---

## Architecture

```
NYC Open Data (Socrata API)
        │
        ▼
  Ingestion pipeline (Python, chunked/streamed)
        │
        ▼
   BigQuery (raw_311_requests)
        │
   ┌────┼─────────────┬─────────────────┐
   ▼    ▼             ▼                 ▼
Anomaly  Trend      Looker Studio    LangChain + Gemini
Detection Analysis   Dashboard       NL Interface
(Colab)  (SQL)                            │
                                           ▼
                                    Eval Harness
```

---

## Key design decisions

- **Live data, not a static dump.** The initial plan used BigQuery's public `bigquery-public-data.new_york_311` dataset, but that dataset turned out to be frozen at 2021 — no longer updated. Switched to ingesting directly from NYC Open Data's live Socrata API instead, which meant building a real ingestion pipeline (with pagination, chunked writes, and incremental loading) rather than just querying a pre-loaded table. More setup work, but a genuinely more useful and demonstrable skill.
- **Day-of-week-aware anomaly baseline.** A naive rolling 7-day average blends weekday and weekend volume together, which misclassifies ordinary weekend dips as anomalies. The detector instead compares each day only to the same weekday over recent prior weeks.
- **Eval harness kept intentionally "imperfect."** The eval suite currently passes 4/5 test cases. The one failure (see Failure #6 below) is a real, demonstrated regression and is kept in the suite on purpose, as a live example of the harness catching something a casual test would have missed.

---

## Known limitations

- The eval harness's known-answer set is small (5 cases) — a production system would need broader coverage across more question types and edge cases.
- The NL interface can generate an incorrect categorical filter value when a column's exact valid values aren't obvious from context alone (see Failure #6) — the schema context given to the model does not currently include sample values for categorical columns like `complaint_type`.
- Ingestion currently pulls a fixed ~6-month window rather than running as a continuously scheduled job.
- Running on Gemini's free tier means the NL interface is constrained by daily/per-minute request quotas — not an issue for demo use, but a real constraint noted and worked around during development (see Failures #5 and #6).

---

## Failure modes found and fixed

Full write-ups with root cause and fix for each: [`failure-mode-log.md`](./failure-mode-log.md)

A quick summary — six real issues were found, diagnosed, and fixed (or, in one case, deliberately left as a documented gap) over the course of the build:

1. **Unbounded memory growth** during ingestion caused a Colab session crash at ~2.5M records — fixed by streaming batches directly to BigQuery instead of accumulating them in memory.
2. **Silent data loss** from a `WRITE_TRUNCATE` config applied on every batch instead of just the first — each batch wiped out the previous ones, and the job reported success throughout. Fixed by scoping truncate-vs-append logic to batch number.
3. **A rolling anomaly baseline that included the anomaly itself**, diluting its own detection threshold and causing a real, visually obvious spike to go unflagged. Fixed by shifting the baseline window to exclude the current day.
4. **Small-sample variance blowup** in a day-of-week-aware baseline — early in the dataset, too few historical points produced near-zero standard deviations and nonsensical z-scores (one as extreme as -555). Fixed with a minimum-history threshold before scoring.
5. **Gemini free-tier rate limiting** during batch testing of the NL interface — fixed with retry-with-backoff logic and request pacing.
6. **A cascading model-swap failure** — a daily quota limit forced a model switch, which hit a deprecated model, then a response-format change between model versions, and finally surfaced a genuine SQL-generation accuracy regression that the eval harness caught and the manual testing had missed.

The pattern worth noting across all six: several of these failed *silently* — no exception, no crash, just quietly wrong output (#2, #3, #6). That's the harder failure mode to catch, and the recurring fix wasn't a single code change so much as a practice: validate outputs against a known-correct answer, not just against "the code ran without an error."

---

## Setup

1. Create a BigQuery dataset and set your project ID.
2. Run the ingestion notebook (`notebooks/ingestion.ipynb`) to pull live 311 data from NYC Open Data into BigQuery.
3. Run the analysis notebook (`notebooks/anomaly_trend_analysis.ipynb`) for anomaly detection and trend queries.
4. Connect the Looker Studio dashboard to your BigQuery dataset (or use the included dashboard link).
5. Add a Gemini API key as a Colab secret (`GEMINI_API_KEY`) to run the NL interface notebook (`notebooks/nl_interface.ipynb`).
6. Run the eval harness cell in the same notebook to reproduce the evaluation results.

---

## Project structure

```
├── README.md
├── failure-mode-log.md
├── notebooks/
│   ├── ingestion.ipynb
│   ├── anomaly_trend_analysis.ipynb
│   └── nl_interface.ipynb
└── dashboard/
    └── (Looker Studio link / export)
```

---

## Author

Allen Myles — [LinkedIn](https://linkedin.com/in/allenmylesmba) · [GitHub](https://github.com/AMyles17)
