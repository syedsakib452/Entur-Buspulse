"""Local storage, Spark configuration and shared transformations for Silver.

The active pipeline reads date-first Parquet files from TABLES_DIR/bronze-layer and writes
one consolidated Parquet file per table and service date to TABLES_DIR/silver-layer.
DATA_ROOT is scratch space for Spark's intermediate partitioned writes and completion markers.
Swift is intentionally not used after the Bronze stage.
"""
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

RAW = "buspulse-raw"  # retained as a logical name for helper compatibility; Silver does not read it
BRONZE = "buspulse-bronze"
SILVER = "buspulse-silver"
LOCAL_TZ = "Europe/Oslo"
GTFS_SEARCH_DAYS = 7
SETTLE_HOURS = float(os.getenv("SETTLE_HOURS", "3"))


def tables_dir():
    """Root containing tables/bronze-layer and tables/silver-layer."""
    value = os.getenv("TABLES_DIR")
    return Path(value).expanduser() if value else None


def root(bucket):
    """Private Spark scratch root; final deliverables are exported under TABLES_DIR."""
    return str(Path(os.getenv("DATA_ROOT", ".silver-runtime")) / bucket)


def path(bucket, *parts):
    """Resolve a Bronze/Silver partition to the project's date-first local layout."""
    tables = tables_dir()
    if tables and bucket in (BRONZE, SILVER) and parts:
        table = parts[0]
        partition = next(
            (part for part in parts[1:] if part.startswith(("date=", "service_date="))),
            None,
        )
        if partition:
            day = partition.split("=", 1)[1]
            layer = "bronze-layer" if bucket == BRONZE else "silver-layer"
            return str(tables / layer / day / f"{table}.parquet")
    return str(Path(root(bucket), *parts))


def spark_session(app):
    os.environ.setdefault("PYSPARK_PYTHON", os.environ.get("PYTHON", "python"))
    builder = (SparkSession.builder.appName(app)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1400m"))
        .config("spark.local.dir", os.getenv("SPARK_LOCAL_DIR", "/tmp/spark"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        # Write timestamps as standard INT64 microseconds (UTC), not Spark's old INT96 default.
        # INT96 comes back from pyarrow as nanoseconds without a time zone, and Spark then could
        # not read the exported silver files again ("column: [visit_time], physicalType: INT64").
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def _local_path(p):
    return Path(p).expanduser()


def exists(spark, p):
    return _local_path(p).exists()


def has_files(spark, p, suffix=""):
    """True if p is a file with the suffix or a directory containing one."""
    target = _local_path(p)
    if target.is_file():
        return not suffix or target.name.endswith(suffix)
    if not target.is_dir():
        return False
    return any(f.is_file() and (not suffix or f.name.endswith(suffix))
               for f in target.iterdir())


def list_files(spark, p, suffix=""):
    target = _local_path(p)
    if target.is_file():
        return [str(target)] if not suffix or target.name.endswith(suffix) else []
    if not target.is_dir():
        return []
    return sorted(str(f) for f in target.iterdir()
                  if f.is_file() and (not suffix or f.name.endswith(suffix)))


def mark_done(spark, stage, day):
    """Write an internal marker and (for Silver outputs) a user-visible local marker."""
    marker = Path(root(SILVER), "_done", stage, str(day))
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.touch()
    if stage in ("clean", "stop_analysis") and silver_local_dir():
        local_marker = silver_local_dir() / str(day) / f"_done_{stage}"
        local_marker.parent.mkdir(parents=True, exist_ok=True)
        local_marker.touch()


def done_age_hours(spark, stage, day):
    marker = Path(root(SILVER), "_done", stage, str(day))
    if not marker.exists():
        return None
    when = datetime.fromtimestamp(marker.stat().st_mtime, timezone.utc)
    return (datetime.now(timezone.utc) - when).total_seconds() / 3600


def is_done(spark, stage, day):
    if stage in ("clean", "stop_analysis") and silver_local_dir():
        return (silver_local_dir() / str(day) / f"_done_{stage}").is_file()
    return (Path(root(SILVER), "_done", stage, str(day))).is_file()


def day_complete(day, settle_hours=None):
    """True once the UTC day ended and its collector uploads have settled."""
    end = datetime.combine(date.fromisoformat(str(day)) + timedelta(days=1),
                           datetime.min.time(), tzinfo=timezone.utc)
    hours = SETTLE_HOURS if settle_hours is None else float(settle_hours)
    return datetime.now(timezone.utc) >= end + timedelta(hours=hours)


def bronze_ready(spark, day, table="stop_visits"):
    return day_complete(day, SETTLE_HOURS + 3) and has_files(
        spark, path(BRONZE, table, f"date={day}"), ".parquet")


def shift(day, n):
    return (date.fromisoformat(str(day)) + timedelta(days=n)).isoformat()


def nearest_partition(spark, bucket, table, day, key="date", search=GTFS_SEARCH_DAYS):
    for offset in [0] + [s * n for n in range(1, search + 1) for s in (-1, 1)]:
        candidate = shift(day, offset)
        candidate_path = path(bucket, table, f"{key}={candidate}")
        if has_files(spark, candidate_path, ".parquet"):
            if offset:
                print(f"  {table}: none for {day}, using {candidate}")
            return candidate_path
    raise RuntimeError(f"no {table} within {search} days of {day}")


def ts(col):
    c = F.regexp_replace(F.trim(F.col(col).cast("string")), r"(\.\d{6})\d+", "$1")
    return F.when(c == "", None).otherwise(F.to_timestamp(c))


def local_time(col):
    return F.from_utc_timestamp(F.col(col) if isinstance(col, str) else col, LOCAL_TZ)


def local_fields(col, prefix="local"):
    t = local_time(col)
    hour = F.hour(t)
    return {
        f"{prefix}_date": F.date_format(t, "yyyy-MM-dd"),
        f"{prefix}_hour": hour,
        "weekday": F.date_format(t, "EEEE"),
        "period": (F.when(hour.isNull(), None).when(hour <= 5, "night")
                   .when(hour <= 8, "am_peak").when(hour <= 14, "midday")
                   .when(hour <= 17, "pm_peak").otherwise("evening")),
    }


def blank_to_null(col):
    value = F.col(col)
    return F.when(F.trim(value.cast("string")) == "", None).otherwise(value)


def to_bool(col):
    value = F.lower(F.trim(F.col(col).cast("string")))
    return F.coalesce(value.isin("true", "1", "t", "yes"), F.lit(False))


def epoch_s(col):
    return F.unix_micros(F.col(col)) / 1e6


def gtfs_seconds(col):
    parts = F.split(F.trim(F.col(col)), ":")
    return (parts.getItem(0).cast("long") * 3600
            + parts.getItem(1).cast("long") * 60
            + parts.getItem(2).cast("long"))


def write(df, bucket, table, key, value, files=None):
    """Write an internal Spark partition and atomically export a single local Parquet file."""
    parts = (table, f"{key}={value}")
    out = Path(root(bucket), *parts)
    if out.exists():
        import shutil
        shutil.rmtree(out) if out.is_dir() else out.unlink()
    out.parent.mkdir(parents=True, exist_ok=True)
    (df.coalesce(files) if files else df).write.mode("overwrite").parquet(str(out))
    if bucket == SILVER and key == "service_date" and silver_local_dir():
        export_local(out, table, value)
    return str(out)


def silver_local_dir():
    value = os.getenv("SILVER_LOCAL_DIR")
    if value:
        return Path(value).expanduser()
    tables = tables_dir()
    return tables / "silver-layer" if tables else None


def export_local(partition_dir, table, day):
    """Stream Spark part files into one date-first Parquet file without loading the day in RAM."""
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
    source = Path(partition_dir)
    parquet_files = sorted(p for p in source.glob("*.parquet") if p.is_file())
    if not parquet_files:
        raise FileNotFoundError(f"No parquet part files were written to {source}")

    dataset = ds.dataset([str(p) for p in parquet_files], format="parquet")
    # Every timestamp as microseconds in UTC: the one form Spark, pandas and DuckDB all read back
    # (older part files may hold INT96, which pyarrow turns into nanoseconds without a time zone).
    utc_us = pa.timestamp("us", tz="UTC")
    source_schema = pa.schema([f.with_type(utc_us) if pa.types.is_timestamp(f.type) else f
                               for f in dataset.schema])
    add_service_date = "service_date" not in source_schema.names
    output_schema = (source_schema.append(pa.field("service_date", pa.string()))
                     if add_service_date else source_schema)
    destination = silver_local_dir() / str(day) / f"{table}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(destination.name + ".tmp")
    row_count = 0
    try:
        with pq.ParquetWriter(temp, output_schema, compression="zstd") as writer:
            scanner = dataset.scanner(batch_size=100_000, use_threads=True)
            for batch in scanner.to_batches():
                table_part = pa.Table.from_batches([batch]).cast(source_schema, safe=False)  # ns -> us, UTC
                if add_service_date:
                    table_part = table_part.append_column(
                        "service_date", pa.array([str(day)] * batch.num_rows, type=pa.string()))
                writer.write_table(table_part, row_group_size=batch.num_rows)
                row_count += batch.num_rows
        os.replace(temp, destination)
    except Exception:
        temp.unlink(missing_ok=True)
        raise
    print(f"  local copy: {destination} ({row_count:,} rows)")
