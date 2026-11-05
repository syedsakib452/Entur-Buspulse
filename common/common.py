"""Shared settings for the Spark jobs: Spark session, storage paths, dates and helpers.

Storage
    In the cluster every container in Swift is read and written through its S3 API
    (s3a://<container>/...). Spark then reads only the days and columns a job needs,
    straight from object storage, and writes its output the same way: no local copies.
    For tests, DATA_ROOT=/some/folder makes every "container" a local folder instead.

Memory
    Spark runs in local mode inside one pod. Large steps (de-duplicating millions of
    snapshot rows, joins) are sorted and spilled to disk by Spark instead of being held
    in memory; per-trip and per-vehicle maths runs in pandas on one small group at a time.
    The settings below are sized for a pod with about 3 GB of memory.
"""
import os
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F

RAW = os.getenv("RAW_BUCKET", "buspulse-raw")
BRONZE = os.getenv("BRONZE_BUCKET", "buspulse-bronze")
SILVER = os.getenv("SILVER_BUCKET", "buspulse-silver")
LOCAL_TZ = "Europe/Oslo"            # Norway's time zone (also Stavanger); agency_timezone in GTFS
GTFS_SEARCH_DAYS = 7                # use a neighbouring day's timetable if a day has none


# ------------------------------------------------------------------ session
def spark_session(app):
    b = (SparkSession.builder.appName(app)
         .master(os.getenv("SPARK_MASTER", "local[2]"))
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
    endpoint = os.getenv("S3_ENDPOINT")
    if endpoint and not os.getenv("DATA_ROOT"):
        b = (b.config("spark.hadoop.fs.s3a.endpoint", endpoint)
             .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("S3_REGION", "us-east-1"))
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
             .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
             .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .config("spark.hadoop.fs.s3a.connection.timeout", "120000")       # ms: no request waits forever
             .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
             .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
             .config("spark.hadoop.fs.s3a.retry.limit", "10")
             .config("spark.hadoop.fs.s3a.connection.maximum", "32")
             .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
             .config("spark.hadoop.fs.s3a.change.detection.mode", "none"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


# ------------------------------------------------------------------ paths
def root(bucket):
    """s3a://<bucket> in the cluster, <DATA_ROOT>/<bucket> in tests."""
    local = os.getenv("DATA_ROOT")
    return f"{local.rstrip('/')}/{bucket}" if local else f"s3a://{bucket}"


def path(bucket, *parts):
    return "/".join([root(bucket), *parts])


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
    fs, hp = _fs(spark, path(SILVER, "_done", stage, day))
    fs.create(hp, True).close()


def is_done(spark, stage, day):
    return exists(spark, path(SILVER, "_done", stage, day))


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
    """Overwrite one partition: <bucket>/<table>/<key>=<value>/. Reruns replace exactly that day."""
    out = path(bucket, table, f"{key}={value}")
    (df.coalesce(files) if files else df).write.mode("overwrite").parquet(out)
    return out
