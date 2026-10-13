"""Combine GA4 daily and intraday event DataFrames."""

import logging

from pyspark.sql import DataFrame

def combine_properties(
    daily_df: DataFrame,
    intraday_df: DataFrame,
) -> DataFrame:
    """Prefer daily events for each property and date."""
    keys = ["ga4_property_id", "event_date"]

    daily_dates = daily_df.select(*keys).distinct()

    intraday_only = intraday_df.join(
        daily_dates,
        on=keys,
        how="left_anti",
    )

    return daily_df.unionByName(intraday_only, allowMissingColumns=True)
