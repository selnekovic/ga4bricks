"""Orchestrate the GA4 daily events pipeline."""

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

from ga4bricks.bronze.config import GA4Property
from ga4bricks.bronze.reader import read_properties
from ga4bricks.bronze.combine import combine_properties
from ga4bricks.bronze.writer import write_properties
from ga4bricks.bronze.migrate import migrate_schema

logger = logging.getLogger(__name__)


def _date_chunks(
    start_date: date,
    end_date: date,
    chunk_days: int,
) -> list[tuple[date, date]]:
    """Split an inclusive date range into contiguous chunks of at most chunk_days."""
    chunks: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def run_bronze_pipeline(
    spark: SparkSession,
    *,
    properties: list[GA4Property],
    catalog: str,
    lookback_days: int = 3,
    mode: str = "incremental",
    process_date: date | None = None,
    chunk_days: int = 30,
) -> None:
    """Read, combine, and write GA4 events for all configured properties.

    For each property, reads daily and intraday events for the date window
    [start_date, end_date], prefers daily over intraday records, and writes
    the result to ``target_table`` using replaceWhere for safe slice
    replacement.  Each chunk is written independently so a failure
    mid-backfill preserves completed chunks — rerun to resume.

    In incremental mode process_date is the end of the window:
        [process_date - lookback_days, process_date].
    In backfill mode process_date is the start of the window:
        [process_date, today], split into chunks of chunk_days for
        scalable processing.

    Args:
        spark: Active SparkSession.
        properties: List of GA4Property configs to process.
        catalog: Unity Catalog name — the bronze target table is derived as
            {catalog}.ga4_bronze.ga4_events_unioned.
        lookback_days: Number of days before process_date to include
            (inclusive). Used only in incremental mode.
        mode: Processing mode — "incremental" (default) uses the
            lookback_days window; "backfill" uses process_date as the
            start date for a custom historical range.
        process_date: Processing date — end of window in incremental mode
            (defaults to today in each property's timezone if None);
            start of window in backfill mode (required).
        chunk_days: Maximum days per write chunk. Each chunk is read,
            combined, and written independently. Defaults to 30.
    """
    target_table = f"{catalog}.ga4_bronze.ga4_events_unioned"

    if not properties:
        raise ValueError("properties must be a non-empty list")

    property_ids = [p.property_id for p in properties]
    if len(property_ids) != len(set(property_ids)):
        raise ValueError("Configure properties with unique property IDs")

    if lookback_days < 0:
        raise ValueError("lookback_days must be non-negative")

    if mode not in ("incremental", "backfill"):
        raise ValueError(f"mode must be 'incremental' or 'backfill', got {mode!r}")

    if mode == "backfill" and process_date is None:
        raise ValueError("process_date is required when mode is 'backfill'")

    if chunk_days < 1:
        raise ValueError("chunk_days must be at least 1")

    total_writes = 0

    for prop in properties:
        today = datetime.now(ZoneInfo(prop.timezone)).date()

        if mode == "backfill":
            start_date = process_date
            end_date = today
        else:
            end_date = process_date or today
            start_date = end_date - timedelta(days=lookback_days)

        if start_date > end_date:
            raise ValueError("start_date must be on or before end_date")

        for chunk_start, chunk_end in _date_chunks(start_date, end_date, chunk_days):
            logger.info(
                "Reading property %s: %s through %s",
                prop.property_id, chunk_start, chunk_end,
            )

            daily_df = read_properties(
                spark,
                table_name=prop.daily_table,
                property_id=prop.property_id,
                data_source="daily",
                start_date=chunk_start,
                end_date=chunk_end,
            )

            if prop.intraday_table and spark.catalog.tableExists(prop.intraday_table):
                intraday_df = read_properties(
                    spark,
                    table_name=prop.intraday_table,
                    property_id=prop.property_id,
                    data_source="intraday",
                    start_date=chunk_start,
                    end_date=chunk_end,
                )
            else:
                logger.info("No intraday source for property %s", prop.property_id)
                intraday_df = daily_df.limit(0)

            combined_df = combine_properties(daily_df, intraday_df)

            migrate_schema(spark, source_df=combined_df, target_table=target_table)

            write_properties(
                combined_df,
                target_table=target_table,
                replacement_ranges=[(prop.property_id, chunk_start, chunk_end)],
            )
            total_writes += 1

    logger.info(
        "Pipeline complete: %d write(s) across %d property(s)",
        total_writes, len(properties),
    )