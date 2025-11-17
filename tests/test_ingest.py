from dataclasses import dataclass
from datetime import datetime

import ingest


@dataclass
class FakeSource:
    name: str
    reader: callable
    table_name: str
    cleaner: callable
    loader: callable
    validator_table: str


def test_run_pipeline_with_single_source(monkeypatch):
    """Pipeline processes a single source and aggregates stats correctly."""

    # Fake reader returns two records
    def fake_reader():
        return [
            {"timestamp": datetime.now(), "value": 1},
            {"timestamp": datetime.now(), "value": 2},
        ]

    # Use real validate_batch semantics, but short-circuit by returning all valid
    def fake_validate_batch(records, table_name):
        # Return all as valid, none invalid
        return records, []

    # Cleaner just passes through and tags table
    def fake_cleaner(record):
        return {**record, "cleaned": True}

    # Loader pretends to insert everything with no DB rejects
    def fake_loader(records):
        assert all(r.get("cleaned") for r in records)
        return len(records), []

    # Capture rejected records logged by pipeline
    logged_rejects = {}

    def fake_log_rejected_records(rejects):
        logged_rejects["count"] = len(rejects)
        return len(rejects)

    # Fake table counts
    def fake_check_table_counts():
        return {"external_factors": 2, "user_tracking": 0, "stg_rejects": logged_rejects.get("count", 0)}

    source = FakeSource(
        name="test_source",
        reader=fake_reader,
        table_name="external_factors",
        cleaner=fake_cleaner,
        loader=fake_loader,
        validator_table="external_factors",
    )

    monkeypatch.setattr(ingest, "validate_batch", fake_validate_batch)
    monkeypatch.setattr(ingest, "log_rejected_records", fake_log_rejected_records)
    monkeypatch.setattr(ingest, "check_table_counts", fake_check_table_counts)

    result = ingest.run_pipeline(sources=[source])

    assert result["success"] is True
    assert result["sources_processed"] == 1
    assert result["total_read"] == 2
    assert result["total_validated"] == 2
    assert result["total_loaded"] == 2
    assert result["total_rejected"] == 0
    assert isinstance(result["duration_seconds"], float)
    assert len(result["source_stats"]) == 1

    source_stats = result["source_stats"][0]
    assert source_stats["name"] == "test_source"
    assert source_stats["read"] == 2
    assert source_stats["validated"] == 2
    assert source_stats["loaded"] == 2
    assert source_stats["rejected"] == 0
    assert source_stats["db_rejected"] == 0


def test_run_pipeline_with_validation_rejects(monkeypatch):
    """Pipeline aggregates validation rejects and logs them."""

    # One good, one bad record
    def fake_reader():
        return [
            {"timestamp": datetime.now(), "value": 1},
            {"timestamp": datetime.now(), "value": -1},  # invalid
        ]

    def fake_validate_batch(records, table_name):
        valid = [r for r in records if r["value"] > 0]
        invalid = [
            {
                "record": r,
                "errors": [f"value={r['value']} failed validation"],
                "table": table_name,
            }
            for r in records
            if r["value"] <= 0
        ]
        return valid, invalid

    def fake_cleaner(record):
        return {**record, "cleaned": True}

    # Loader accepts all valid records (no DB-level rejects here)
    def fake_loader(records):
        if not records:
            return 0, []
        inserted = len(records)
        db_rejected = []
        return inserted, db_rejected

    logged_rejects = {}

    def fake_log_rejected_records(rejects):
        logged_rejects["items"] = rejects
        return len(rejects)

    def fake_check_table_counts():
        return {}

    source = FakeSource(
        name="test_source_with_rejects",
        reader=fake_reader,
        table_name="external_factors",
        cleaner=fake_cleaner,
        loader=fake_loader,
        validator_table="external_factors",
    )

    monkeypatch.setattr(ingest, "validate_batch", fake_validate_batch)
    monkeypatch.setattr(ingest, "log_rejected_records", fake_log_rejected_records)
    monkeypatch.setattr(ingest, "check_table_counts", fake_check_table_counts)

    result = ingest.run_pipeline(sources=[source])

    assert result["success"] is True
    assert result["total_read"] == 2
    assert result["total_validated"] == 1  # only one valid
    # Only 1 validation reject
    assert result["total_rejected"] == 1

    source_stats = result["source_stats"][0]
    assert source_stats["read"] == 2
    assert source_stats["validated"] == 1
    assert source_stats["loaded"] == 1
    assert source_stats["rejected"] == 1  # validation reject
    assert source_stats["db_rejected"] == 0

    # Ensure rejected records were passed through to log_rejected_records
    assert "items" in logged_rejects
    assert len(logged_rejects["items"]) == 1


