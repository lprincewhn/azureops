import pytest
import exporter


@pytest.fixture(autouse=True)
def reset_logs_client():
    """Clear the cached LogsIngestionClient singleton after every test."""
    yield
    exporter._logs_client = None
