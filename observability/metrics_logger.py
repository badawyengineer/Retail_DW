"""
observability/metrics_logger.py

Every pipeline notebook (Bronze/Silver/Gold/validation) calls log_event()
after each load or check. It doesn't talk to any observability backend
directly — it just prints one structured JSON line per event to stdout.

That's a deliberate choice: on Databricks, notebook stdout already lands in
the cluster's driver logs, which is exactly the kind of local log source
Cribl Edge is built to tail and forward. Nothing here depends on Cribl
being present — the notebooks work identically (you just don't get
centralized observability) if it isn't. See observability/README.md for
how the two sides connect.
"""

import json
import time


def log_event(event_type: str, **fields):
    """
    Emit one structured JSON log line for a pipeline event.

    event_type: short label, e.g. "bronze_load", "silver_load",
                "validation_check", "gold_build".
    **fields:   whatever's relevant to that event — table name, row count,
                which check, pass/fail, etc. Kept as free-form kwargs
                rather than a fixed schema, since the fields genuinely
                differ per event type and forcing a shared schema would
                mean a lot of null padding.
    """
    record = {
        "event_type": event_type,
        "pipeline": "retail_dwh",
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **fields,
    }
    # A plain print, not a Python logging call — Cribl Edge tails raw
    # stdout/log files rather than hooking into Python's logging module,
    # so the simplest thing that actually gets collected is a print.
    print(json.dumps(record))
    return record
