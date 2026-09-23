# Failure Mode Log — Civic Operations Intelligence Platform

Running log of real issues hit during the build, how they were diagnosed, and how they were fixed. Kept for the README's "governance/reliability" section (Week 6).

---

## Failure #1: Colab session crash — unbounded memory growth during ingestion

**Date:** Week 1, initial ingestion pass

**What happened:**
The first version of the NYC 311 ingestion script pulled paginated data from the Socrata API (50,000 records per request) and accumulated every batch into a single Python list (`all_records`) before converting the whole thing to a pandas DataFrame at the end. After successfully pulling ~2.5 million records, the Colab session crashed with "session crashed after using all available RAM."

**Root cause:**
Two compounding issues:
1. **Unbounded accumulation** — the script held every batch in memory simultaneously instead of processing/writing incrementally, so memory usage grew linearly with total record count and was never released until the very end (if it ever got there).
2. **Wrong date window (contributing factor)** — the `$where` filter used a hardcoded date (`2025-02-01`) intended to mean "6 months back," but the actual current date was August 2026. This silently pulled ~18 months of data instead of 6, roughly tripling the intended volume and accelerating the crash.

**Fix:**
- Rewrote the ingestion loop to **write each batch directly to BigQuery** (`load_table_from_dataframe` with `WRITE_APPEND`) immediately after fetching it, then explicitly deleted the batch DataFrame (`del df_batch`) to free memory right away. This keeps peak memory usage roughly constant (~1 batch's worth) regardless of total dataset size, instead of growing without bound.
- Corrected the date filter to use the actual current date, restoring the intended 6-month window.

**Why this matters / what it signals:**
This is a common real-world failure pattern in data pipelines — "works fine at small scale, silently fails at production scale" — and the fix (streaming/incremental writes instead of load-then-process) is the standard mitigation. Documenting it here rather than glossing over it demonstrates the pipeline was tested against a realistic volume, not just a toy sample.

---

## Failure #2: `WRITE_TRUNCATE` applied to every batch, silently destroying prior data

**Date:** Week 1, ingestion cleanup pass (after fixing Failure #1)

**What happened:**
After correcting the date window from Failure #1, the reload appeared to succeed (query completed, no errors) — but the verification query showed only **31,235 rows**, spanning just **Aug 2–5, 2026**, instead of the expected ~5 million rows covering the full Feb 2026–Aug 2026 window.

**Root cause:**
The `job_config` (with `write_disposition="WRITE_TRUNCATE"`) was defined once, outside the loop, and reused for every batch load inside the `while True` loop. `WRITE_TRUNCATE` doesn't mean "truncate once at the start" — it means "replace the table's entire contents with this load job," and it was applied on **every single batch**, not just the first. Because the API returned records in ascending `created_date` order, each new batch wiped out everything the previous batches had written. The table only ever ended up holding whatever the *last* batch processed happened to contain — the last few days of data.

This is a subtle bug because it produces no error at all. The job reports success on every batch, and the final row count only reveals the problem when checked against the expected total.

**Fix:**
Moved `write_disposition` selection inside the loop, keyed on batch number: `WRITE_TRUNCATE` only for `batch_num == 0` (the first batch, to clear out old/stale data once), and `WRITE_APPEND` for every batch after that (to accumulate correctly).

```python
write_mode = "WRITE_TRUNCATE" if batch_num == 0 else "WRITE_APPEND"
job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition=write_mode)
```

**Why this matters / what it signals:**
This is the kind of bug that wouldn't show up in a small test (e.g. a 1-batch pull) — it only surfaces at multi-batch scale, and it fails *silently* rather than throwing an error, which makes it more dangerous than a crash. It's a good example of why **verifying row counts and date ranges after every load**, not just checking for job success, is a necessary practice — "the job succeeded" and "the data is correct" are not the same thing. Worth calling out explicitly in the README's reliability section as a reminder that pipeline success signals (green checkmarks, no exceptions) don't guarantee data correctness.

---

## Failure #3: Rolling baseline included the anomaly itself, masking the anomaly it was meant to detect

**Date:** Week 2, first anomaly detection pass

**What happened:**
Built a rolling 7-day mean/std z-score detector to flag unusual daily request volumes. On the first run, it reported **zero anomalies** across 185 days — despite a visually obvious spike already identified in the daily volume chart (a day with roughly double the normal request count, around Feb 23-24, 2026). Manually inspecting the top z-scores (ignoring the threshold) showed the spike day's z-score was only ~2.18 — high, but not high enough to clear the 2.5 threshold, even though the underlying jump was dramatic (15,424 requests against a ~10,700 baseline).

**Root cause:**
The rolling window (`daily_df['request_count'].rolling(window=7).mean()`, `center=False`) included the *current day itself* as one of the 7 days used to compute that day's own baseline mean and standard deviation. On a genuine spike day, this meant the spike inflated its own comparison baseline — pulling the rolling mean upward and, more damagingly, inflating the rolling standard deviation (which nearly tripled on the spike day, from a typical ~500-1500 up to ~5000). A larger std directly shrinks the z-score denominator's sensitivity, so the anomaly effectively diluted the very yardstick used to measure it. This is a known but easy-to-miss pitfall in naive rolling-window anomaly detection: the point being evaluated must never be part of its own baseline.

**Fix:**
Added `.shift(1)` before the `.rolling()` call, so each day's baseline is computed only from the 7 days *strictly before* it, excluding the day being scored:
```python
daily_df['rolling_mean'] = daily_df['request_count'].shift(1).rolling(window=7).mean()
daily_df['rolling_std'] = daily_df['request_count'].shift(1).rolling(window=7).std()
```
After the fix, the same spike day's z-score jumped from ~2.18 to **8.27** — correctly identified as a major outlier — and total flagged anomalies went from 0 to 10 (a mix of genuine spikes and dips).

**Why this matters / what it signals:**
This bug is more dangerous than a crash or a data-loss bug (Failures #1 and #2) because it fails silently *and* undermines the specific thing the system exists to do — it doesn't just lose data, it actively suppresses the detection of the anomalies it was built to catch, and does so more severely on larger anomalies (since a bigger spike contaminates its own baseline more). It's a good illustration of why validating a detection system against a known, visually-confirmed anomaly (rather than trusting "the code ran and returned zero" as a success signal) is an essential step, not an optional one.

---

## Failure #4: Small-sample variance produced nonsensical z-scores in the day-of-week-aware baseline

**Date:** Week 2, day-of-week-aware detector improvement

**What happened:**
After confirming the naive 7-day rolling detector was flagging two Saturdays (2026-05-23 and 2026-07-11) as anomalies, investigation showed both were part of a genuine but explainable pattern — NYC 311 volume is structurally lower on weekends, and these two Saturdays fell on holiday-adjacent weekends (Memorial Day weekend, the weekend after July 4th). Rather than accept the naive detector's blended weekday/weekend baseline as a real limitation, built an improved version that compares each day only to the same weekday over the preceding 4 occurrences (e.g., Saturdays compared only to other Saturdays).

The improved version initially reported **27 anomalies**, several with absurd z-scores — one day scored **z = -555.68**, and several others were in the range of -7 to -11, despite fairly ordinary-looking request counts. These were concentrated in the first 2-3 weeks of the dataset.

**Root cause:**
Early in the 6-month window, some weekdays only had 1-2 prior same-weekday observations available to build a baseline from. With so few data points, the sample standard deviation could be extremely small purely by chance (e.g., two nearly-identical prior Tuesdays produce a std close to zero). Dividing by a near-zero standard deviation in the z-score formula caused the score to blow up to an extreme, meaningless value. The logic was mathematically correct but statistically unreliable without enough historical points to estimate variance meaningfully.

**Fix:**
Added a minimum history requirement (`min_history = 3`) before computing a day-of-week z-score for any given day; days without at least 3 prior same-weekday observations are left unscored (`None`) rather than producing an unreliable ratio. Anomaly flagging then explicitly drops unscored rows before reporting results.

**Why this matters / what it signals:**
This is a second, distinct instance of the same underlying class of bug as Failure #3 (a statistic computed from too little or contaminated data producing a misleadingly extreme result) — but caught through a different mechanism: rather than validating against a known ground-truth anomaly, this one was caught by noticing an implausible score (-555 on an ordinary day) during a routine "does this look right" scan of the output. Together, these two failures form a natural before/after pair for the README: the naive detector missed a real anomaly (Failure #3), and its day-of-week-aware fix initially manufactured fake ones (Failure #4) — both traceable to the same root cause (rolling statistics that don't yet have a reliable sample to work from), and both requiring a "does this pass the smell test" review rather than treating "the code ran" as sufficient validation.

---

## Failure #5: Gemini free-tier rate limit hit during batch testing of the NL interface

**Date:** Week 5, LangChain + Gemini natural language interface

**What happened:**
Built a working NL-to-SQL-to-answer pipeline (`ask_nyc311()`) using LangChain and Gemini 2.5 Flash: a user's plain-English question is converted to BigQuery SQL, the SQL is executed, and the result is summarized back in plain English. The first three test questions ran successfully with correct, sensible answers (e.g., "There were a total of 334,691 requests in February 2026," "The Bronx had the most heating complaints"). The fourth question in the same batch failed with a `429 RESOURCE_EXHAUSTED` / `GoogleRateLimitError`.

**Root cause:**
Gemini's free tier enforces a hard rate limit of 5 requests per minute for `gemini-2.5-flash`. Each call to `ask_nyc311()` makes *two* separate Gemini API calls under the hood — one to generate the SQL query from the question, and a second to turn the query result into a plain-English answer. Running four test questions back-to-back in a tight loop meant 8 Gemini calls in rapid succession, exceeding the 5-per-minute ceiling on the fourth question.

**Fix:**
Added retry-with-backoff logic to the answer-generation step (catches `RESOURCE_EXHAUSTED`/`429` errors specifically and waits progressively longer between retries), and added a `time.sleep(12)` delay between test questions in the batch-testing loop to stay comfortably under the 5-requests-per-minute ceiling given that each question consumes 2 of those requests.

**Why this matters / what it signals:**
This is a real, practical constraint of building on a free-tier LLM API that doesn't show up in a single successful test call — it only surfaces under realistic usage patterns like batch testing or multiple users querying close together. It's directly relevant to the eval harness (Week 6): any automated evaluation that runs multiple test questions in sequence needs to account for rate limits, either through pacing or retry logic, or the evaluation results will be contaminated by infrastructure failures rather than reflecting the model's actual answer quality.

---

## Failure #6: Model-swap cascade — quota exhaustion, deprecation, response-format break, and a SQL accuracy regression caught by the eval harness

**Date:** Week 5-6, LangChain + Gemini natural language interface and eval harness

**What happened:**
While testing the NL interface with a 5-question batch, hit Gemini's free-tier daily quota (`GenerateRequestsPerDayPerProjectPerModel-FreeTier`, limit: 20 requests/day) for `gemini-2.5-flash`. Switching to `gemini-2.5-flash-lite` failed immediately with a 404 — that model had been deprecated and was no longer available to new users. Switching again to `gemini-3.5-flash-lite` connected successfully, but broke both `generate_sql()` and `ask_nyc311()` with `AttributeError: 'list' object has no attribute 'strip'/'lower'` — this model version returns `.content` as a list of content parts rather than a plain string, an undocumented response-format difference between model versions. After fixing the format handling with a shared `extract_text()` helper, the pipeline ran successfully — but when the formal eval harness (5 questions with known expected answers) was run against this new model, one question failed: "Which borough had the most heating complaints?" returned "no data available" because the model generated `WHERE complaint_type = 'HEATING'`, a plausible-sounding but incorrect literal value — the actual category in the dataset is `'HEAT/HOT WATER'`. The same question had passed correctly under `gemini-2.5-flash` earlier in the session.

**Root cause:**
Three independent, stacked causes: (1) the free tier's daily request quota is small enough that normal iterative development and testing exhausts it easily; (2) Google deprecates specific model versions without long-lived backward compatibility, requiring code changes mid-project; (3) different model versions structure their API response objects differently (string vs. list content), which is not something client code can safely assume stays constant across a model swap. Layered on top of these infrastructure issues, the *forced* model swap (undertaken only to route around the quota/deprecation problems) introduced a genuine accuracy regression: the newer, lighter model was less reliable at inferring the correct categorical value from context and guessed a wrong-but-plausible string instead.

**Fix:**
Infrastructure issues: added a shared `extract_text()` helper to normalize response content regardless of format, used by both `generate_sql()` and `ask_nyc311()`. Accuracy issue: this is intentionally left unfixed as a demonstrated, documented gap rather than silently patched — the eval harness's job is to catch exactly this kind of regression, and the failing case is kept in the eval set going forward as a regression check. A production fix would involve enriching the schema context with actual sample values for key categorical columns (like `complaint_type`) so the model has known-valid values to match against instead of guessing.

**Why this matters / what it signals:**
This is the single most valuable failure in the log because it demonstrates the full point of building an eval harness in the first place: a change made for a legitimate operational reason (working around quota and deprecation issues) silently degraded output quality, and only a systematic, repeatable eval with known-correct answers caught it — a one-off manual test would very plausibly have missed it, since the failure only appears on a specific question requiring exact category knowledge. It also demonstrates real infrastructure resilience under a realistic, unglamorous constraint: building on a free-tier third-party API means the ground can shift under you (quotas, deprecations, response formats) even when your own code hasn't changed.
