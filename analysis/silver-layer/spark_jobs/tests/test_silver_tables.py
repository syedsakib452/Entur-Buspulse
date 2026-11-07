from spark_jobs import common
from spark_jobs.run_silver_tables import _requested_days


def test_local_tables_paths_use_the_project_layer_names(tmp_path, monkeypatch):
    monkeypatch.setenv("TABLES_DIR", str(tmp_path))
    assert common.path(common.BRONZE, "stop_visits", "date=2026-11-01") == str(
        tmp_path / "bronze-layer" / "2026-11-01" / "stop_visits.parquet")
    assert common.path(common.SILVER, "clean_stop_visits", "service_date=2026-11-01") == str(
        tmp_path / "silver-layer" / "2026-11-01" / "clean_stop_visits.parquet")


def test_open_ended_range_discovers_local_bronze_dates(tmp_path):
    (tmp_path / "2026-11-01").mkdir()
    (tmp_path / "2026-11-02").mkdir()
    (tmp_path / "not-a-date").mkdir()
    assert _requested_days(tmp_path, "2026-11-02", "") == ["2026-11-02"]


def test_closed_range_includes_both_dates(tmp_path):
    assert _requested_days(tmp_path, "2026-11-01", "2026-11-03") == [
        "2026-11-01", "2026-11-02", "2026-11-03"]


def test_exported_silver_file_is_readable_by_spark_again(tmp_path, monkeypatch):
    """Regression: Spark read its own exported file back in stop_analysis and failed with
    'column: [visit_time], physicalType: INT64, logicalType: timestamp' (INT96 -> nanoseconds)."""
    import pytest
    pytest.importorskip("pyspark")
    import pyarrow.parquet as pq
    from datetime import datetime, timezone
    monkeypatch.setenv("SILVER_LOCAL_DIR", str(tmp_path / "silver-layer"))
    monkeypatch.setenv("SPARK_LOCAL_DIR", str(tmp_path / "spark"))
    spark = common.spark_session("export-test")
    t = datetime(2026, 10, 25, 1, 30, tzinfo=timezone.utc)
    df = spark.createDataFrame([("T1", t), ("T2", None)], "trip_id string, visit_time timestamp")
    for kind in ("INT96", "TIMESTAMP_MICROS"):          # old part files and new ones
        spark.conf.set("spark.sql.parquet.outputTimestampType", kind)
        part_dir = tmp_path / kind
        df.coalesce(1).write.mode("overwrite").parquet(str(part_dir))
        common.export_local(part_dir, f"table_{kind}", "2026-10-24")
        out = tmp_path / "silver-layer" / "2026-10-24" / f"table_{kind}.parquet"
        field = pq.read_schema(out).field("visit_time").type
        assert str(field) == "timestamp[us, tz=UTC]"
        back = spark.read.parquet(str(out))
        micros = back.selectExpr("max(unix_micros(visit_time))").first()[0]   # the read that used to fail
        assert micros == int(t.timestamp() * 1_000_000)
        assert {r.service_date for r in back.collect()} == {"2026-10-24"}
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")


def test_export_skips_empty_part_files(tmp_path, monkeypatch):
    """Regression (2026-10-20): Spark wrote an empty part file and the export failed with
    'Row group size cannot be 0'. Empty parts are skipped; an all-empty table still gives a file."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    monkeypatch.setenv("SILVER_LOCAL_DIR", str(tmp_path / "silver-layer"))
    schema = pa.schema([("trip_id", pa.string()), ("visit_time", pa.timestamp("us", tz="UTC"))])
    parts = tmp_path / "parts"
    parts.mkdir()
    pq.write_table(pa.table({"trip_id": ["T1"], "visit_time": [None]}, schema=schema), parts / "part-0.parquet")
    pq.write_table(schema.empty_table(), parts / "part-1.parquet")
    common.export_local(parts, "t", "2026-10-20")
    out = pq.read_table(tmp_path / "silver-layer" / "2026-10-20" / "t.parquet")
    assert out.num_rows == 1 and out.column("service_date").to_pylist() == ["2026-10-20"]

    only_empty = tmp_path / "only_empty"
    only_empty.mkdir()
    pq.write_table(schema.empty_table(), only_empty / "part-0.parquet")
    common.export_local(only_empty, "e", "2026-10-20")
    assert pq.read_table(tmp_path / "silver-layer" / "2026-10-20" / "e.parquet").num_rows == 0
