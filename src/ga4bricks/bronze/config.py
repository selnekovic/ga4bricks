"""Configuration dataclasses and YAML loader for GA4 property pipelines."""

from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class GA4Property:
    """Configuration for a single GA4 property.

    Attributes:
        property_id: GA4 property identifier (numeric string).
        daily_table: Fully qualified Unity Catalog table for daily events.
        intraday_table: Fully qualified table for intraday events,
            or None if intraday ingestion is disabled.
        timezone: IANA timezone name for resolving the current calendar day.
    """

    property_id: str
    daily_table: str
    intraday_table: str | None
    timezone: str
