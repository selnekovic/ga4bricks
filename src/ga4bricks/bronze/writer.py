"""Write GA4 events to Unity Catalog Delta tables."""

import logging
import re
from collections.abc import Sequence
from datetime import date

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def write_properties(
    df: DataFrame,
    *,
    target_table: str,
    replacement_ranges: Sequence[tuple[str, date, date]],
) -> None:
    """Atomically replace complete snapshots within supplied ranges.

    Each range contains: (property_id, start_date, end_date).
    Both dates are inclusive.

    The caller must verify source completeness for every range.
    Dates without incoming rows are also cleared within those ranges.
    """
    if not isinstance(target_table, str) or not target_table.strip():
        raise ValueError("target_table must be a non-empty string")

    if not replacement_ranges:
        logger.info("No replacement ranges supplied; skipping")
        return

    predicates = []

    for property_id, start_date, end_date in replacement_ranges:
        if not isinstance(property_id, str) or not re.fullmatch(
            r"[0-9]+", property_id
        ):
            raise ValueError("Invalid GA4 property ID")

        if type(start_date) is not date or type(end_date) is not date:
            raise ValueError(
                "start_date and end_date must be datetime.date values"
            )

        if start_date > end_date:
            raise ValueError("start_date must be on or before end_date")

        predicates.append(
            f"(ga4_property_id = '{property_id}' "
            f"AND event_date BETWEEN '{start_date:%Y%m%d}' "
            f"AND '{end_date:%Y%m%d}')"
        )

    logger.info(
        "Replacing %d property/date range(s) in %s",
        len(predicates),
        target_table,
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", " OR ".join(predicates))
        .saveAsTable(target_table)
    )

    logger.info("Write completed for %s", target_table)
