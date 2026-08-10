# Observability

## What's actually here

`metrics_logger.py` — a small `log_event()` helper that every notebook
(Bronze, Silver, Gold, validation, profiling) calls after each load or
check. It doesn't connect to anything — it just prints one structured
JSON line per event, e.g.:

```json
{"event_type": "silver_load", "pipeline": "retail_dwh", "timestamp_utc": "2026-08-10T12:00:00Z", "table": "crm_cust_info", "row_count": 18484}
```

On Databricks, notebook `print()` output lands in the cluster's driver
logs. That's the design: a plain print is the simplest thing that ends up
somewhere a log collector can actually reach, without wiring the pipeline
to a specific observability backend.

`cribl_edge_source.yml` — an example Cribl Edge Source config that tails
those driver logs and picks out the JSON event lines specifically (not
every stray print in the cluster).

## What this gives you

Once wired up, every load and every validation check becomes a queryable
event in whatever your Cribl output is pointed at (Splunk, S3, Elastic,
etc.) — not just something visible in a single notebook run:

- Row counts over time per table, per layer — catches a source feed
  quietly shrinking or growing.
- Validation pass/fail history — catches a rule that used to pass
  starting to fail on a specific date, not just "it's broken right now."
- One event stream across Bronze/Silver/Gold instead of three separate
  notebook run histories.

## Honest limitation

**This was written to spec, not verified against a live Cribl Edge
instance** — there's no Cribl deployment in this environment to test the
Source config against. Same caveat as the rest of the pipeline: the logic
is correct as written, but the exact driver log path, the JSON line
breaker rule name, and the destination all depend on your actual
workspace/Cribl setup and should be confirmed before relying on it in
production.

If you don't have Cribl Edge available at all, nothing else in this repo
depends on it — the notebooks run identically, you just lose the
centralized event history and fall back to reading each notebook's own
printed output.
