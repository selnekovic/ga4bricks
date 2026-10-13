# ga4bricks

A lightweight Python library for transforming Google Analytics 4 (GA4) BigQuery export data in Databricks.

## Overview

`ga4bricks` works with GA4 event data already ingested into Delta tables by the managed Databricks connector. It provides reusable PySpark transformations and a simple pipeline for consolidating daily and intraday events from multiple GA4 properties into one Bronze table. Silver-layer data cleaning and Gold-layer aggregations are planned for future work.

## Initial design

- **Inputs:** Daily and intraday GA4 event tables in Unity Catalog.
- **Output:** A unified Bronze Delta events table.
- **Multi-property support:** Include `ga4_property_id` on every output record.
- **Source tracking:** Use `data_source` (`daily` or `intraday`).
- **Daily refresh:** Reprocess the current calendar day and the previous three days; prefer daily records over intraday records for each property and date.
- **Safe replacement:** Replace only the affected property/date slices, preserving other properties and dates.

## Project structure

```text
ga4bricks/
├── README.md
├── pyproject.toml
├── uv.lock
├── .gitignore
├── ga4bricks_runtime   # Databricks notebook
├── src/
│   └── ga4bricks/
│       ├── __init__.py
│       ├── bronze/
│       │   ├── __init__.py
│       │   ├── config.py
│       │   ├── pipeline.py
│       │   ├── reader.py
│       │   ├── combine.py
│       │   └── writer.py
│       ├── silver/
│       │   └── __init__.py
│       └── gold/
│           └── __init__.py
└── tests/
    ├── conftest.py
    ├── test_reader.py
    └── test_combine.py
```

## Configuration

Pipeline configuration is defined as Python constants directly in the `ga4bricks_runtime` notebook:

- **`CATALOG`** — Unity Catalog name. The bronze target table is derived as `{catalog}.ga4_bronze.ga4_events_unioned`.
- **`DEFAULT_TIMEZONE`** — IANA timezone applied to all properties unless overridden per-property.
- **`PROPERTIES`** — list of `GA4Property` instances, each with:
  - `property_id` (required) — GA4 property identifier (numeric string).
  - `daily_table` (required) — fully qualified Unity Catalog table for daily events.
  - `intraday_table` (optional) — fully qualified table for intraday events, or `None` to disable.
  - `timezone` (required) — IANA timezone name for resolving the current calendar day.

Run-specific parameters (`process_date`, `mode`) are passed as notebook widgets, not stored in the library.

## Running the pipeline

The `ga4bricks_runtime` notebook orchestrates the bronze pipeline. It installs the `ga4bricks` package from the workspace path, registers widgets, and calls `run_bronze_pipeline()`.

### Modes

- **`incremental`** (default) — processes a lookback window ending at `process_date` (defaults to today if empty); The window spans `lookback_days` days (default 3). Daily records are preferred over intraday records for each property and date.
- **`backfill`** — processes a historical range from `process_date` to today, split into chunks of `chunk_days` days (default 30). Each chunk is written independently so a failure mid-backfill preserves completed chunks — rerun to resume.

### Widgets

| Widget | Type | Description |
| --- | --- | --- |
| `process_date` | text | Date in `YYYY-MM-DD` format. End of window in incremental mode (defaults to today if empty); start of window in backfill mode (required). |
| `mode` | dropdown | `incremental` or `backfill`. |

`lookback_days` and `chunk_days` are optional arguments with defaults in `run_bronze_pipeline()` — override by passing them explicitly if needed.

## Public API

The package exports the following symbols from `ga4bricks`:

| Symbol | Description |
| --- | --- |
| `GA4Property` | Frozen dataclass for configuring a single GA4 property. |
| `run_bronze_pipeline` | Orchestrates read, combine, and write for all configured properties. |
| `read_properties` | Reads GA4 events from a Delta table with date filtering. |
| `combine_properties` | Merges daily and intraday DataFrames, preferring daily records. |
| `write_properties` | Writes combined events to the target table using `replaceWhere` for safe slice replacement. |

## Status

Bronze layer (raw read, daily/intraday merge, Delta write) and pipeline orchestration are implemented. Silver-layer data cleaning and Gold-layer aggregations are scaffolded for future work.
