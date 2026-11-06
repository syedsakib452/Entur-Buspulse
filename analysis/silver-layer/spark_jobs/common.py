"""Shared settings for the Spark jobs: Spark session, storage paths, dates and helpers.

Storage
    Swift containers are staged to a local cache with python-swiftclient. Spark reads and
    writes local files; completed output files are uploaded to Swift. DATA_ROOT=/some/folder
    uses local folders without Swift and is intended for tests.

Memory
    Spark runs in local mode inside one pod. Large steps (de-duplicating millions of
    snapshot rows, joins) are sorted and spilled to disk by Spark instead of being held
    in memory; per-trip and per-vehicle maths runs in pandas on one small group at a time.
    The settings below are sized for a pod with about 3 GB of memory.
"""
import os
from pathlib import Path
import shutil
import sys
import threading
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession, functions as F
from swiftclient.client import Connection
from swiftclient.exceptions import ClientException

RAW = os.getenv("RAW_BUCKET", "buspulse-raw")
BRONZE = os.getenv("BRONZE_BUCKET", "buspulse-bronze")
SILVER = os.getenv("SILVER_BUCKET", "buspulse-silver")
LOCAL_TZ = "Europe/Oslo"            # Norway's time zone (also Stavanger); agency_timezone in GTFS
GTFS_SEARCH_DAYS = 7                # use a neighbouring day's timetable if a day has none
# a UTC day counts as complete (all its raw files uploaded) this long after it ends
SETTLE_HOURS = float(os.getenv("SETTLE_HOURS", "3"))
SWIFT_CACHE = Path(os.getenv("SWIFT_CACHE_DIR", "/tmp/buspulse-swift"))
_swift_local = threading.local()
_downloaded = set()
_downloaded_lock = threading.Lock()


def swift_enabled():
    return not os.getenv("DATA_ROOT")


def swift_connection():
    """Create a Swift connection from the OpenStack environment used by the collectors."""
    env = os.environ
    if not env.get("OS_AUTH_URL"):
        raise RuntimeError(
            "Swift storage needs OS_AUTH_URL and OpenStack credentials. "
            "Activate the credential-loaded environment or set DATA_ROOT for local data."
        )
    return Connection(
        authurl=env["OS_AUTH_URL"],
        user=env.get("OS_USERNAME"),
        key=env.get("OS_PASSWORD"),
        auth_version="3",
        timeout=120,
        retries=5,
        os_options={k: v for k, v in {
            "auth_type": env.get("OS_AUTH_TYPE"),
            "application_credential_id": env.get("OS_APPLICATION_CREDENTIAL_ID"),
            "application_credential_secret": env.get("OS_APPLICATION_CREDENTIAL_SECRET"),
            "project_id": env.get("OS_PROJECT_ID"),
            "project_name": env.get("OS_PROJECT_NAME"),
            "user_domain_name": env.get("OS_USER_DOMAIN_NAME"),
            "user_domain_id": env.get("OS_USER_DOMAIN_ID"),
            "project_domain_name": env.get("OS_PROJECT_DOMAIN_NAME"),
            "project_domain_id": env.get("OS_PROJECT_DOMAIN_ID"),
            "region_name": env.get("OS_REGION_NAME"),
            "interface": env.get("OS_INTERFACE"),
        }.items() if v},
    )


def _swift():
    if not hasattr(_swift_local, "connection"):
        _swift_local.connection = swift_connection()
    return _swift_local.connection


def _swift_list(bucket, prefix):
    try:
        _, objects = _swift().get_container(bucket, prefix=prefix, full_listing=True)
        return objects
    except ClientException as error:
        if error.http_status == 404:
            return []
        raise


def _stage_prefix(bucket, prefix):
    local_root = SWIFT_CACHE / bucket
    target = local_root / prefix if prefix else local_root
    target.mkdir(parents=True, exist_ok=True)
    objects = _swift_list(bucket, prefix)
    live_names = set()
    for obj in objects:
        name = obj.get("name")
        if not name or name.endswith("/"):
            continue
        live_names.add(name)
        local_file = local_root / name
        local_file.parent.mkdir(parents=True, exist_ok=True)
        signature = (bucket, name, obj.get("hash"), obj.get("bytes"))
        with _downloaded_lock:
            if signature in _downloaded and local_file.exists():
                continue
        _, body = _swift().get_object(bucket, name, resp_chunk_size=1024 * 1024)
        temp_file = local_file.with_name(local_file.name + ".swift-tmp")
        with temp_file.open("wb") as stream:
            for chunk in body:
                stream.write(chunk)
        temp_file.replace(local_file)
        with _downloaded_lock:
            _downloaded.add(signature)
    if target.is_dir():
        for local_file in target.rglob("*"):
            if local_file.is_file():
                name = local_file.relative_to(local_root).as_posix()
                if name not in live_names:
                    local_file.unlink()
    return str(target)


def _upload_prefix(bucket, prefix, local_path):
    base = Path(local_path)
    files = sorted(path for path in base.rglob("*") if path.is_file())
    uploaded = set()
    for path in files:
        name = f"{prefix.rstrip('/')}/{path.relative_to(base).as_posix()}".lstrip("/")
        with path.open("rb") as stream:
            _swift().put_object(
                bucket, name, contents=stream, content_length=path.stat().st_size,
                content_type="application/vnd.apache.parquet" if path.suffix == ".parquet"
                else "application/octet-stream",
            )
        uploaded.add(name)
    for obj in _swift_list(bucket, prefix.rstrip("/") + "/"):
        name = obj.get("name")
        if name and name not in uploaded:
            _swift().delete_object(bucket, name)
    with _downloaded_lock:
        _downloaded.difference_update(
            item for item in _downloaded if item[0] == bucket and item[1].startswith(prefix)
        )


def check_container(bucket):
    try:
        _swift().head_container(bucket)
        return True
    except ClientException as error:
        if error.http_status == 404:
            return False
        raise


def ensure_container(bucket):
    try:
        _swift().put_container(bucket)
    except ClientException as error:
        if error.http_status != 409:
            raise
        if not check_container(bucket):
            raise error


def raw_feeds():
    _, entries = _swift().get_container(RAW, delimiter="/")
    return sorted(item["subdirectory"].rstrip("/") for item in entries
                  if item.get("subdirectory"))


# ------------------------------------------------------------------ session
def spark_session(app):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    b = (SparkSession.builder.appName(app)
         .master(os.getenv("SPARK_MASTER", "local[2]"))
         # Every timestamp is a UTC instant. Spark otherwise shows and converts timestamps in the
         # computer's own time zone, so the same job would give other local hours on a laptop in
         # Norway than in the cluster. Pinned to UTC, the only conversion is local_time() below.
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1400m"))
         .config("spark.local.dir", os.getenv("SPARK_LOCAL_DIR", "/tmp/spark"))      # spill space
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
         .config("spark.sql.files.maxPartitionBytes", "64m")
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")   # date=... stays text
         .config("spark.sql.execution.arrow.pyspark.enabled", "true")
         .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000")
         .config("spark.sql.parquet.compression.codec", "zstd")
         .config("spark.sql.legacy.parquet.nanosAsLong", "true")      # pandas-written ns timestamps
         .config("spark.sql.ansi.enabled", "false")                   # bad values -> null, like errors="coerce"
         .config("spark.ui.enabled", "false")
         .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


# ------------------------------------------------------------------ paths
def root(bucket):
    """Local data root or the local staging cache for a Swift container."""
    local = os.getenv("DATA_ROOT")
    return str(Path(local) / bucket) if local else str(SWIFT_CACHE / bucket)


def path(bucket, *parts):
    if swift_enabled():
        return _stage_prefix(bucket, "/".join(parts))
    return str(Path(root(bucket), *parts))


def _fs(spark, p):
    jvm = spark._jvm
    hp = jvm.org.apache.hadoop.fs.Path(p)
    return hp.getFileSystem(spark._jsc.hadoopConfiguration()), hp


def exists(spark, p):
    fs, hp = _fs(spark, p)
    return fs.exists(hp)


def has_files(spark, p, suffix=""):
    """True if the folder exists and holds at least one file (ending in `suffix`)."""
    fs, hp = _fs(spark, p)
    if not fs.exists(hp):
        return False
    it = fs.listFiles(hp, False)
    while it.hasNext():
        if it.next().getPath().getName().endswith(suffix):
            return True
    return False


def list_files(spark, p, suffix=""):
    fs, hp = _fs(spark, p)
    if not fs.exists(hp):
        return []
    out, it = [], fs.listFiles(hp, False)
    while it.hasNext():
        st = it.next()
        if st.getPath().getName().endswith(suffix):
            out.append(st.getPath().toString())
    return sorted(out)


def mark_done(spark, stage, day):
    """Small marker object: <silver>/_done/<stage>/<day>. Written last, so it only exists
    when every output of that stage and day was written."""
    if swift_enabled():
        _swift().put_object(SILVER, f"_done/{stage}/{day}", contents=b"")
    else:
        fs, hp = _fs(spark, path(SILVER, "_done", stage, day))
        fs.create(hp, True).close()
    if stage in ("clean", "stop_analysis") and silver_local_dir():
        # local marker: tables/silver-layer/<day>/_done_<stage> means that stage's files are complete
        marker = silver_local_dir() / str(day) / f"_done_{stage}"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()


def done_age_hours(spark, stage, day):
    """Hours since <stage> of <day> was marked done, or None if it is not done."""
    if swift_enabled():
        from email.utils import parsedate_to_datetime
        try:
            headers = _swift().head_object(SILVER, f"_done/{stage}/{day}")
        except ClientException as error:
            if error.http_status == 404:
                return None
            raise
        when = parsedate_to_datetime(headers["last-modified"])
    else:
        p = Path(root(SILVER), "_done", stage, day)
        if not p.exists():
            return None
        when = datetime.fromtimestamp(p.stat().st_mtime, timezone.utc)
    return (datetime.now(timezone.utc) - when).total_seconds() / 3600


def is_done(spark, stage, day):
    if swift_enabled():
        try:
            _swift().head_object(SILVER, f"_done/{stage}/{day}")
            return True
        except ClientException as error:
            if error.http_status == 404:
                return False
            raise
    return exists(spark, path(SILVER, "_done", stage, day))


# ------------------------------------------------------------------ readiness
def day_complete(day, settle_hours=None):
    """True once UTC day `day` ended at least SETTLE_HOURS ago, so the collectors have uploaded
    every raw file of it. Building bronze earlier would freeze a partial day."""
    end = datetime.combine(date.fromisoformat(day_str(day)) + timedelta(days=1), datetime.min.time(),
                           tzinfo=timezone.utc)
    hours = SETTLE_HOURS if settle_hours is None else settle_hours
    return datetime.now(timezone.utc) >= end + timedelta(hours=hours)


def bronze_ready(spark, day, table="stop_visits"):
    """Bronze of `day` is safe to read: the Spark bronze job finished it (done marker), or, for
    bronze made by the older script (no markers), the day is complete and its files are there."""
    if is_done(spark, "bronze", day):
        return True
    return day_complete(day, SETTLE_HOURS + 3) and \
        has_files(spark, path(BRONZE, table, f"date={day}"), ".parquet")


# ------------------------------------------------------------------ dates
def day_str(d):
    return d.isoformat() if isinstance(d, date) else str(d)


def shift(day, n):
    return (date.fromisoformat(day_str(day)) + timedelta(days=n)).isoformat()


def nearest_partition(spark, bucket, table, day, key="date", search=GTFS_SEARCH_DAYS):
    """The partition of `table` for `day`, or the closest one before or after it
    (a GTFS timetable covers several weeks, so a neighbouring day's is fine)."""
    for offset in [0] + [s * n for n in range(1, search + 1) for s in (-1, 1)]:
        cand = shift(day, offset)
        p = path(bucket, table, f"{key}={cand}")
        if has_files(spark, p, ".parquet"):
            if offset:
                print(f"  {table}: none for {day}, using {cand}")
            return p
    raise RuntimeError(f"no {table} within {search} days of {day}")


# ------------------------------------------------------------------ columns
def ts(col):
    """ISO 8601 text (with offset or Z, sometimes 9 decimals) -> UTC timestamp. Unparseable -> null.
    Spark keeps microseconds, so extra digits are cut first."""
    c = F.regexp_replace(F.trim(F.col(col).cast("string")), r"(\.\d{6})\d+", "$1")
    return F.when(c == "", None).otherwise(F.to_timestamp(c))


def local_time(col):
    """UTC instant -> Norwegian wall-clock time, row by row with the Europe/Oslo rules, so each row
    gets +02:00 or +01:00 for its own date (DST ends 25 Oct 2026 03:00 -> 02:00).
    Correct only because the session time zone is pinned to UTC (spark_session). Use it once, on
    an original UTC column, and only to derive local_* fields: never overwrite the UTC column."""
    return F.from_utc_timestamp(F.col(col) if isinstance(col, str) else col, LOCAL_TZ)


def local_fields(col, prefix="local"):
    """local_date, local_hour, weekday (Monday ...), period, all from one UTC column."""
    t = local_time(col)
    h = F.hour(t)
    return {f"{prefix}_date": F.date_format(t, "yyyy-MM-dd"),
            f"{prefix}_hour": h,
            "weekday": F.date_format(t, "EEEE"),
            "period": (F.when(h.isNull(), None).when(h <= 5, "night").when(h <= 8, "am_peak").when(h <= 14, "midday")
                       .when(h <= 17, "pm_peak").otherwise("evening"))}


def blank_to_null(col):
    c = F.col(col)
    return F.when(F.trim(c.cast("string")) == "", None).otherwise(c)


def to_bool(col):
    """true/1/t/yes (any case) -> True, everything else (incl. missing) -> False."""
    s = F.lower(F.trim(F.col(col).cast("string")))
    return F.coalesce(s.isin("true", "1", "t", "yes"), F.lit(False))


def epoch_s(col):
    """Timestamp -> seconds since 1970 (double)."""
    return F.unix_micros(F.col(col)) / 1e6


def gtfs_seconds(col):
    """GTFS 'HH:MM:SS' (HH can be 24 or more) -> seconds."""
    p = F.split(F.trim(F.col(col)), ":")
    return p.getItem(0).cast("long") * 3600 + p.getItem(1).cast("long") * 60 + p.getItem(2).cast("long")


def write(df, bucket, table, key, value, files=None):
    """Overwrite one partition, uploading it to Swift when Swift storage is enabled.
    Silver tables are also exported to SILVER_LOCAL_DIR/<date>/<table>.parquet when that is set."""
    parts = (table, f"{key}={value}")
    out = str(Path(root(bucket), *parts))
    if swift_enabled():
        shutil.rmtree(out, ignore_errors=True)
        Path(out).mkdir(parents=True, exist_ok=True)
    (df.coalesce(files) if files else df).write.mode("overwrite").parquet(out)
    if swift_enabled():
        _upload_prefix(bucket, "/".join(parts), out)
    if bucket == SILVER and key == "service_date" and silver_local_dir():
        export_local(out, table, value)
    return out


# ------------------------------------------------------------------ local silver copy
def silver_local_dir():
    """Folder for a local copy of the silver tables, e.g. <repo>/tables/silver-layer (unset = none).
    Read at call time, so tests and the Docker container can set it."""
    d = os.getenv("SILVER_LOCAL_DIR")
    return Path(d) if d else None


def export_local(partition_dir, table, day):
    """One silver partition -> SILVER_LOCAL_DIR/<day>/<table>.parquet, a single file with a
    service_date column: the layout build_gold.py reads (SILVER_DIR=tables/silver-layer/<day>).
    Written to a temporary name first, so a reader never sees half a file."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    t = pq.read_table(partition_dir)                     # ignores Spark's _SUCCESS / .crc files
    if "service_date" not in t.column_names:
        t = t.append_column("service_date", pa.array([str(day)] * t.num_rows, pa.string()))
    dest = silver_local_dir() / str(day) / f"{table}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    pq.write_table(t, tmp, compression="zstd")
    os.replace(tmp, dest)
    print(f"  local copy: {dest} ({t.num_rows:,} rows)")
