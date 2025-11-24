import ingest


def _patch_ingest(
    monkeypatch,
    fake_external_flow,
    fake_user_flow,
    fake_log_rejected_records,
    fake_check_table_counts,
):
    monkeypatch.setattr(ingest, "_run_external_factors_flow", fake_external_flow)
    monkeypatch.setattr(ingest, "_run_user_tracking_flow", fake_user_flow)
    monkeypatch.setattr(ingest, "log_rejected_records", fake_log_rejected_records)
    monkeypatch.setattr(ingest, "check_table_counts", fake_check_table_counts)


def test_run_pipeline_happy_path(monkeypatch):
    def fake_external_flow():
        return (
            {
                "name": "external_factors",
                "read": 1,
                "validated": 1,
                "rejected": 0,
                "loaded": 1,
                "db_rejected": 0,
            },
            [],
        )

    def fake_user_flow():
        return (
            {
                "name": "user_tracking",
                "read": 2,
                "validated": 2,
                "rejected": 0,
                "loaded": 2,
                "db_rejected": 0,
            },
            [],
        )

    logged_rejects = {}

    def fake_log_rejected_records(rejects):
        logged_rejects["count"] = len(rejects)
        return len(rejects)

    def fake_check_table_counts():
        return {"external_factors": 1, "user_tracking": 2, "stg_rejects": logged_rejects.get("count", 0)}

    _patch_ingest(
        monkeypatch,
        fake_external_flow,
        fake_user_flow,
        fake_log_rejected_records,
        fake_check_table_counts,
    )

    result = ingest.run_pipeline()

    assert result["success"] is True
    assert result["sources_processed"] == 2
    assert result["total_read"] == 3
    assert result["total_validated"] == 3
    assert result["total_loaded"] == 3
    assert result["total_rejected"] == 0
    assert isinstance(result["duration_seconds"], float)
    assert len(result["source_stats"]) == 2

    names = {s["name"] for s in result["source_stats"]}
    assert names == {"external_factors", "user_tracking"}


def test_run_pipeline_with_validation_rejects(monkeypatch):
    def fake_external_flow():
        return (
            {
                "name": "external_factors",
                "read": 1,
                "validated": 0,
                "rejected": 1,
                "loaded": 0,
                "db_rejected": 0,
            },
            [{"table": "external_factors", "record": {}, "error": "bad external"}],
        )

    def fake_user_flow():
        return (
            {
                "name": "user_tracking",
                "read": 2,
                "validated": 1,
                "rejected": 1,
                "loaded": 1,
                "db_rejected": 0,
            },
            [{"table": "user_tracking", "record": {}, "error": "bad user"}],
        )

    logged_rejects = {}

    def fake_log_rejected_records(rejects):
        logged_rejects["items"] = rejects
        return len(rejects)

    def fake_check_table_counts():
        return {}

    _patch_ingest(
        monkeypatch,
        fake_external_flow,
        fake_user_flow,
        fake_log_rejected_records,
        fake_check_table_counts,
    )

    result = ingest.run_pipeline()

    assert result["success"] is True
    assert result["total_read"] == 3
    assert result["total_validated"] == 1
    assert result["total_rejected"] == 2

    assert "items" in logged_rejects
    assert len(logged_rejects["items"]) == 2

