"""Read GA4 events from Unity Catalog Delta tables."""

import logging
from datetime import date
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def read_properties(
    spark: SparkSession,
    *,
    table_name: str,
    property_id: str,
    data_source: Literal["daily", "intraday"],
    start_date: date,
    end_date: date,
) -> DataFrame:
    """Read GA4 events for an inclusive date range."""
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")

    if not table_name:
        raise ValueError("table_name must be a non-empty string")

    if not property_id:
        raise ValueError("property_id must be a non-empty string")

    logger.info(
        "Reading %s events for property %s from %s (%s to %s)",
        data_source, property_id, table_name,
        start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"),
    )
    return (
        spark.table(table_name)
        .filter(
            F.col("event_date").between(
                start_date.strftime("%Y%m%d"),
                end_date.strftime("%Y%m%d"),
            )
        )
        .withColumn("ga4_property_id", F.lit(property_id))
        .withColumn("data_source", F.lit(data_source))
    )
