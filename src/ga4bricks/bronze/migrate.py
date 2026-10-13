"""Schema migration for GA4 bronze tables in Unity Catalog."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def migrate_schema(
    spark: SparkSession,
    *,
    source_df: DataFrame,
    target_table: str,
) -> None:
    """Add columns present in *source_df* but missing from *target_table*.

    Compares the source DataFrame schema against the existing Delta
    table and adds any missing columns via ``ALTER TABLE``.  This is
    required for GA4 schema evolution in Unity Catalog, where
    ``mergeSchema`` is blocked by Table ACLs and ``overwriteSchema``
    is incompatible with ``replaceWhere``.

    No-op if the target table does not exist yet — it will be created
    by the subsequent write with the correct schema.

    Args:
        spark: Active SparkSession.
        source_df: DataFrame whose schema to compare against the target.
        target_table: Fully qualified Unity Catalog table name.
    """
    if not spark.catalog.tableExists(target_table):
        return

    existing_cols = {c.name for c in spark.table(target_table).schema.fields}
    source_cols = {c.name: c.dataType for c in source_df.schema.fields}
    missing = {
        name: dtype
        for name, dtype in source_cols.items()
        if name not in existing_cols
    }

    for name, dtype in missing.items():
        ddl_type = dtype.simpleString()
        logger.info("Adding column %s %s to %s", name, ddl_type, target_table)
        spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {name} {ddl_type}")
