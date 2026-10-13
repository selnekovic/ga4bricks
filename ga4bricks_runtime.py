# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # ga4bricks 
# MAGIC
# MAGIC Notebook for running the ga4bricks GA4 data pipeline.

# COMMAND ----------

# DBTITLE 1,Install & Widgets
# Do not edit — installs the ga4bricks package and registers notebook widgets
%pip install "/Workspace/Users/dataops@filviks.com/ga4bricks"
dbutils.library.restartPython()

dbutils.widgets.text("process_date", "")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"])

# COMMAND ----------

# DBTITLE 1,Configuration section header
# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Define GA4 properties, catalog, and timezone settings below.
# MAGIC
# MAGIC `process_date` interpretation depends on mode:
# MAGIC * **incremental**: end of the lookback window (defaults to today if empty)
# MAGIC * **backfill**: start of the historical range up to today (required)

# COMMAND ----------

# DBTITLE 1,Configuration
# ga4bricks pipeline configuration
# Run-specific parameters (process_date, mode) are passed as
# job parameters via notebook widgets, not defined here.

from ga4bricks import GA4Property

# Shared default timezone applied to all properties unless overridden.
DEFAULT_TIMEZONE = "UTC"

# Unity Catalog target for all pipeline layers.
CATALOG = "marketing"

# GA4 properties to process.
# Required: property_id, daily_table
# Optional: intraday_table (None = disabled), timezone (uses default)
PROPERTIES = [
    GA4Property(
        property_id="123456789",
        daily_table="workspace.ga4_property_1.events",
        intraday_table="workspace.ga4_property_1.events_intraday",
        timezone="Europe/Bratislava",
    ),
    GA4Property(
        property_id="987654321",
        daily_table="workspace.ga4_property_2.events",
        intraday_table="workspace.ga4_property_2.events_intraday",
        timezone="Europe/Bratislava",
    ),
]

# COMMAND ----------

# DBTITLE 1,Pipeline section header
# MAGIC %md
# MAGIC ## Pipeline — DO NOT EDIT
# MAGIC
# MAGIC Everything below this point is the pipeline execution. Do not modify these cells.
# MAGIC
# MAGIC `lookback_days` and `chunk_days` are optional arguments:
# MAGIC * `lookback_days=3` — days before run_date to include (incremental mode only)
# MAGIC * `chunk_days=30` — max days per write chunk
# MAGIC
# MAGIC Override by passing them explicitly if needed.

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date

from ga4bricks import run_bronze_pipeline

# COMMAND ----------

# DBTITLE 1,Parse Parameters
# Parse widget parameters passed by the job or set interactively
dbutils.widgets.text("process_date", "")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"])

process_date_text = dbutils.widgets.get("process_date").strip()
mode = dbutils.widgets.get("mode")

process_date = date.fromisoformat(process_date_text) if process_date_text else None

if mode == "backfill" and process_date is None:
    raise ValueError("Backfill requires process_date in YYYY-MM-DD format")

# COMMAND ----------

# DBTITLE 1,Infrastructure setup
if mode == "backfill":
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ga4_bronze")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ga4_silver")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.ga4_gold")

# COMMAND ----------

# DBTITLE 1,Run bronze pipeline
run_bronze_pipeline(
    spark,
    properties=PROPERTIES,
    catalog=CATALOG,
    mode=mode,
    process_date=process_date,
)