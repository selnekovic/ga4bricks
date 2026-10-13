"""Shared test fixtures for ga4bricks."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Provide the SparkSession for tests.

    In Databricks this returns the existing session; locally it creates one.
    """
    return SparkSession.builder.getOrCreate()