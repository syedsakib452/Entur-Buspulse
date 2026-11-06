"""Gold layer job, run by the gold-layer CronJob: finished silver days in Swift -> gold tables in Swift.

1. Finds the silver days that are complete: both <silver>/_done/clean/<day> and
   <silver>/_done/stop_analysis/<day> exist (never a day that silver is still writing).
2. Stops early if those days and their markers are the same as at the last gold build.
3. Downloads those days' silver tables and merges each table into one parquet file
   (service_date comes from the partition folder), the layout build_gold.py expects.
4. Runs build_gold.py (DuckDB) on them.
5. Uploads the result to <gold>/builds/<build id>/ and only then points <gold>/latest.json at it,
   so a reader never sees a half-uploaded gold build. The last KEEP_BUILDS builds are kept.

If build_gold.py fails (for example a reconciliation check), nothing is uploaded and the previous
gold build stays the live one.

Storage: Swift with the OS_* variables (like the collectors), or DATA_ROOT=/folder for local tests.
Usage:  python run_gold.py [--start 2026-10-20] [--end 2026-11-06] [--redo]
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
SILVER = os.getenv("SILVER_BUCKET", "buspulse-silver")
GOLD = os.getenv("GOLD_BUCKET", "buspulse-gold")
WORK = Path(os.getenv("GOLD_WORK_DIR", "/work/gold"))
KEEP_BUILDS = int(os.getenv("KEEP_BUILDS", "3"))
# every silver table gold may use; a missing one is simply not provided (build_gold.py skips it)
SILVER_TABLES = ["clean_stop_visits", "stop_event_status", "expected_per_stop", "stop_gps_status",
                 "trip_vehicle", "per_stop_evidence", "time_lost_null", "stop_headways", "trip_segments"]


# ------------------------------------------------------------------ storage
class Storage:
    """The few object-store operations gold needs, on Swift or on a local folder (DATA_ROOT)."""

    def __init__(self):
        self.local = os.getenv("DATA_ROOT")
        self.conn = None if self.local else self._swift()

    @staticmethod
    def _swift():
        from swiftclient.client import Connection
        env = os.environ
        if not env.get("OS_AUTH_URL"):
            raise RuntimeError("set OS_AUTH_URL and the OpenStack credentials, or DATA_ROOT for local data")
        opts = {k: env.get(v) for k, v in {
            "auth_type": "OS_AUTH_TYPE", "application_credential_id": "OS_APPLICATION_CREDENTIAL_ID",
            "application_credential_secret": "OS_APPLICATION_CREDENTIAL_SECRET",
            "project_id": "OS_PROJECT_ID", "project_name": "OS_PROJECT_NAME",
            "user_domain_name": "OS_USER_DOMAIN_NAME", "user_domain_id": "OS_USER_DOMAIN_ID",
            "project_domain_name": "OS_PROJECT_DOMAIN_NAME", "project_domain_id": "OS_PROJECT_DOMAIN_ID",
            "region_name": "OS_REGION_NAME", "interface": "OS_INTERFACE"}.items()}
        return Connection(authurl=env["OS_AUTH_URL"], user=env.get("OS_USERNAME"), key=env.get("OS_PASSWORD"),
                          auth_version="3", timeout=120, retries=5,
                          os_options={k: v for k, v in opts.items() if v})

    def ensure(self, bucket):
        if self.local:
            Path(self.local, bucket).mkdir(parents=True, exist_ok=True)
        else:
            self.conn.put_container(bucket)

    def list(self, bucket, prefix):
        """-> {name: last_modified} of the objects under prefix."""
        if self.local:
            base = Path(self.local, bucket)
            return {p.relative_to(base).as_posix(): str(p.stat().st_mtime)
                    for p in (base / prefix).parent.rglob("*")
                    if p.is_file() and p.relative_to(base).as_posix().startswith(prefix)} if base.exists() else {}
        from swiftclient.exceptions import ClientException
        try:
            _, objs = self.conn.get_container(bucket, prefix=prefix, full_listing=True)
        except ClientException as e:
            if e.http_status == 404:
                return {}
            raise
        return {o["name"]: o.get("last_modified", "") for o in objs if not o["name"].endswith("/")}

    def download(self, bucket, name, dest):
        dest.parent.mkdir(parents=True, exist_ok=True)
        if self.local:
            shutil.copyfile(Path(self.local, bucket, name), dest)
            return
        _, body = self.conn.get_object(bucket, name, resp_chunk_size=1024 * 1024)
        with dest.open("wb") as f:
            for chunk in body:
                f.write(chunk)

    def read_json(self, bucket, name):
        if self.local:
            p = Path(self.local, bucket, name)
            return json.loads(p.read_text()) if p.exists() else None
        from swiftclient.exceptions import ClientException
        try:
            return json.loads(self.conn.get_object(bucket, name)[1])
        except ClientException as e:
            if e.http_status == 404:
                return None
            raise

    def upload(self, bucket, name, src=None, data=None):
        if self.local:
            p = Path(self.local, bucket, name)
            p.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, p) if src else p.write_bytes(data)
            return
        if src:
            with open(src, "rb") as f:
                self.conn.put_object(bucket, name, contents=f, content_length=os.path.getsize(src))
        else:
            self.conn.put_object(bucket, name, contents=data, content_type="application/json")

    def delete(self, bucket, name):
        if self.local:
            Path(self.local, bucket, name).unlink(missing_ok=True)
        else:
            self.conn.delete_object(bucket, name)


# ------------------------------------------------------------------ job
def ready_days(store, start, end):
    """Silver days with both stages finished, and their marker times (a rebuild changes them)."""
    clean = {n.rsplit("/", 1)[-1]: t for n, t in store.list(SILVER, "_done/clean/").items()}
    gps = {n.rsplit("/", 1)[-1]: t for n, t in store.list(SILVER, "_done/stop_analysis/").items()}
    days = sorted(d for d in clean.keys() & gps.keys() if start <= d <= end)
    only_clean = sorted(d for d in clean.keys() - gps.keys() if start <= d <= end)
    if only_clean:
        print(f"  silver days still waiting for stop_analysis (left out): {only_clean}")
    return {d: [clean[d], gps[d]] for d in days}


def stage_silver(store, days, silver_dir):
    """Download each table's partitions of `days` and merge them into <silver_dir>/<table>.parquet."""
    parts = WORK / "parts"
    con = duckdb.connect()
    for table in SILVER_TABLES:
        n = 0
        for d in days:
            for name in store.list(SILVER, f"{table}/service_date={d}/"):
                if name.endswith(".parquet"):
                    store.download(SILVER, name, parts / name)
                    n += 1
        if not n:
            print(f"  {table}: not in silver, skipped")
            continue
        out = silver_dir / f"{table}.parquet"
        src = (parts / table).as_posix() + "/*/*.parquet"
        con.execute(f"COPY (SELECT * FROM read_parquet('{src}', hive_partitioning = true, union_by_name = true)) "
                    f"TO '{out.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
        rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]
        print(f"  {table}: {n} files -> {rows:,} rows")
    con.close()
    shutil.rmtree(parts, ignore_errors=True)


def publish(store, gold_dir, days):
    build = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    files = sorted(p for p in gold_dir.iterdir() if p.is_file())
    for p in files:
        store.upload(GOLD, f"builds/{build}/{p.name}", src=p)
    pointer = {"build": build, "prefix": f"builds/{build}/", "service_dates": days,
               "files": [p.name for p in files], "built_at_utc": datetime.now(timezone.utc).isoformat()}
    store.upload(GOLD, "latest.json", data=json.dumps(pointer, indent=1).encode())   # switch readers last
    print(f"  uploaded {len(files)} files to {GOLD}/builds/{build}/ and pointed latest.json at it")
    builds = sorted({n.split("/")[1] for n in store.list(GOLD, "builds/")})
    for old in builds[:-KEEP_BUILDS]:
        for n in store.list(GOLD, f"builds/{old}/"):
            store.delete(GOLD, n)
        print(f"  removed old build {old}")


def main():
    today = date.today().isoformat()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default=os.getenv("START_DATE") or "2026-10-20")
    p.add_argument("--end", default=os.getenv("END_DATE") or today)
    p.add_argument("--redo", action="store_true", default=os.getenv("REDO", "").lower() in ("1", "true", "yes"))
    a = p.parse_args()

    store = Storage()
    store.ensure(GOLD)
    days = ready_days(store, a.start, a.end)
    print(f"=== gold {a.start} .. {a.end}: {len(days)} finished silver days {list(days)}", flush=True)
    if not days:
        print("=== nothing to build yet")
        return 0
    state = store.read_json(GOLD, "_state.json")
    if state == days and not a.redo:
        print("=== gold is up to date with silver, nothing to do")
        return 0

    shutil.rmtree(WORK, ignore_errors=True)
    silver_dir, gold_dir = WORK / "silver", WORK / "gold"
    silver_dir.mkdir(parents=True)
    gold_dir.mkdir(parents=True)
    stage_silver(store, list(days), silver_dir)

    env = dict(os.environ, SILVER_DIR=str(silver_dir), GOLD_DIR=str(gold_dir))
    result = subprocess.run([sys.executable, str(HERE / "build_gold.py")], cwd=HERE, env=env)
    if result.returncode != 0:
        print(f"=== build_gold.py failed (exit {result.returncode}): nothing uploaded, previous gold stays live")
        return 1

    publish(store, gold_dir, list(days))
    store.upload(GOLD, "_state.json", data=json.dumps(days).encode())
    shutil.rmtree(WORK, ignore_errors=True)
    print("=== gold done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
