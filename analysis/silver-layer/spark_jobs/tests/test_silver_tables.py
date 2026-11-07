from spark_jobs import common
from spark_jobs.run_silver_tables import _requested_days


def test_local_tables_paths_use_date_first_layout(tmp_path, monkeypatch):
    monkeypatch.setenv("TABLES_DIR", str(tmp_path))

    assert common.path(
        common.BRONZE, "stop_visits", "date=2026-11-01"
    ) == str(tmp_path / "bronze" / "2026-11-01" / "stop_visits.parquet")
    assert common.path(
        common.SILVER, "clean_stop_visits", "service_date=2026-11-01"
    ) == str(tmp_path / "silver" / "2026-11-01" / "clean_stop_visits.parquet")


def test_open_ended_range_discovers_local_bronze_dates(tmp_path):
    (tmp_path / "2026-11-01").mkdir()
    (tmp_path / "2026-11-02").mkdir()
    (tmp_path / "not-a-date").mkdir()

    assert _requested_days(tmp_path, "2026-11-02", "") == ["2026-11-02"]


def test_closed_range_includes_both_dates(tmp_path):
    assert _requested_days(tmp_path, "2026-11-01", "2026-11-03") == [
        "2026-11-01", "2026-11-02", "2026-11-03"
    ]
