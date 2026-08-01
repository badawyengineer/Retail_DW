# Getting this repo running in Databricks Free Edition

The project doc (section 4.1) specifies Databricks Free Edition with Unity
Catalog, serverless compute, notebooks, and a Volume — no cluster and no
Databricks CLI/Asset Bundle setup required. Everything below is doable from
the Workspace UI.

Transform logic in the notebooks is pandas, not PySpark — pandas ships with
the Databricks Runtime already, so no extra library install is needed.

## 1. Load the notebooks into the workspace

Two options:

- **Repos (recommended if submitting via GitHub):** Workspace → Repos →
  Add Repo → paste this repo's GitHub URL. Databricks clones it and the
  four `.py` notebooks under `/notebooks` show up ready to open — no
  manual import needed.
- **Manual import:** Workspace → your folder → Import → select each `.py`
  file under `/notebooks/00_setup`, `/notebooks/01_bronze_ingestion`,
  `/notebooks/02_silver_transformation`, `/notebooks/03_gold_views`, and
  `/tests/validation_tests.py`. Databricks recognizes the
  `# Databricks notebook source` header and the `# COMMAND ----------`
  cell markers automatically, so each file opens as a proper multi-cell
  notebook, not a flat script.

## 2. Run order

1. Open `00_setup` and run all cells — creates the `retail_dwh` catalog,
   the `bronze`/`silver`/`gold` schemas, and the `raw_files` volume.
2. Go to Catalog → `retail_dwh` → `bronze` → `raw_files` (or Catalog Explorer's
   Volume UI) and upload the six CSVs from `/datasets`.
3. Run `01_bronze_ingestion`, then `02_silver_transformation`, then
   `03_gold_tables`, each with "Run All."
4. Run `tests/validation_tests.py` and confirm every check cell comes back
   empty.
5. Gold is now physical tables, not views (pandas can't create a view) — if
   you re-run Silver later, re-run `03_gold_tables` afterward too, or Gold
   will be stale.

## 3. Compute

No cluster to create — attach each notebook to **Serverless** compute
(the default in Free Edition) rather than a classic cluster.
