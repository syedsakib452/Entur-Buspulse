"""The whole batch pipeline, run by the Kubernetes CronJob: raw -> bronze -> silver -> stop analysis.

For every day from START_DATE to END_DATE (empty = yesterday, UTC):
  1. bronze         raw files of day D -> bronze tables                    (once per day)
  2. clean          service date D -> clean_stop_visits, stop_event_status (needs bronze D and D+1)
  3. stop_analysis  service date D -> stop_gps_status, trip_vehicle, ...   (needs clean D)
Finished stages leave a marker (<silver>/_done/<stage>/<day>), so a rerun only does what is missing,
and a failed day is retried on the next run without stopping the other days.

A service date is cleaned only once the next day's bronze exists: its late trips get their final
times after midnight UTC. So the nightly run (04:00 UTC) builds bronze for yesterday and cleans and
analyses the day before.

Reading too early: bronze of a day is built only once that UTC day ended SETTLE_HOURS (3 h) ago, so
every raw file has been uploaded; silver reads only bronze days that carry a done marker. Readers of
silver (gold) should likewise use only days with <silver>/_done/clean/<day>.

Silver delay: with SILVER_DELAY_HOURS=12, silver of day D starts only 12 hours after bronze of D+1
(the last bronze day it needs) was marked done. SILVER_LOCAL_DIR=<repo>/tables/silver-layer also
writes every silver table to <repo>/tables/silver-layer/<date>/<table>.parquet.

Usage:  python -m spark_jobs.run_pipeline [--start 2026-10-20] [--end 2026-10-30] [--stages bronze clean]
        python -m spark_jobs.run_pipeline --start 2026-10-24 --end 2026-10-26 --redo   # rebuild after a fix
"""
import argparse
import os
import sys
import time
import traceback
from datetime import date, datetime, timedelta, timezone

import importlib

from spark_jobs import common


def _stage(module):
    """Imports a stage only when it runs, so each layer's image needs only its own modules
    (the bronze image has no silver code, the silver image no bronze code)."""
    return importlib.import_module(f"spark_jobs.{module}").run

STAGES = ["bronze", "clean", "stop_analysis"]


def days(start, end):
    d, e = date.fromisoformat(start), date.fromisoformat(end)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


def preflight(spark):
    """Fails in the first minute, with a readable message, if storage cannot be reached."""
    try:
        if common.swift_enabled():
            common.ensure_container(common.BRONZE)
            common.ensure_container(common.SILVER)
            missing = [bucket for bucket in (common.RAW, common.BRONZE, common.SILVER)
                       if not common.check_container(bucket)]
            if missing:
                raise RuntimeError(f"Swift containers do not exist or are inaccessible: {missing}")
            feeds = common.raw_feeds()
            print(f"=== Swift storage ok: {common.RAW} holds {feeds}", flush=True)
        else:
            for bucket in (common.RAW, common.BRONZE, common.SILVER):
                common.exists(spark, common.root(bucket) + "/")
            fs, hp = common._fs(spark, common.root(common.RAW) + "/")
            feeds = sorted(st.getPath().getName() for st in fs.listStatus(hp) if st.isDirectory())
            print(f"=== local storage ok: {common.root(common.RAW)} holds {feeds}", flush=True)
    except Exception as e:  # noqa: BLE001
        print(f"=== cannot reach storage: {e}\n"
              "    check the OpenStack OS_* credentials and that the Swift containers "
              "buspulse-raw, buspulse-bronze and buspulse-silver exist.", flush=True)
        spark.stop()
        sys.exit(2)


def main():
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", default=os.getenv("START_DATE") or "2026-10-20")
    p.add_argument("--end", default=os.getenv("END_DATE") or yesterday)
    p.add_argument("--stages", nargs="+", choices=STAGES,
                   default=(os.getenv("STAGES") or " ".join(STAGES)).split())
    p.add_argument("--redo", action="store_true", default=os.getenv("REDO", "").lower() in ("1", "true", "yes"),
                   help="rebuild the days in --start..--end even if they are marked done (e.g. after a fix)")
    a = p.parse_args()

    spark = common.spark_session("buspulse-pipeline")
    preflight(spark)
    failed = []

    def step(stage, day, fn):
        t0 = time.time()
        try:
            result = fn(spark, day)
            if stage == "bronze" and result:
                raise RuntimeError(f"{result} unreadable input files; day not marked done")
            common.mark_done(spark, stage, day)
            print(f"--- {stage} {day}: done in {(time.time() - t0) / 60:.1f} min", flush=True)
            return True
        except Exception as e:  # noqa: BLE001 - a failed day must not stop the others
            failed.append(f"{stage} {day}")
            print(f"--- {stage} {day}: FAILED after {(time.time() - t0) / 60:.1f} min: {e}", flush=True)
            traceback.print_exc()
            return False

    def skip(stage, day):
        return not a.redo and common.is_done(spark, stage, day)

    all_days = list(days(a.start, a.end))
    print(f"=== pipeline {a.start} .. {a.end}, stages {a.stages}{', REDO' if a.redo else ''}", flush=True)

    if "bronze" in a.stages:
        for d in all_days:
            if skip("bronze", d):
                continue
            if not common.day_complete(d):
                # the collectors may still be uploading this day: a bronze built now would miss files
                print(f"--- bronze {d}: day not complete yet (+{common.SETTLE_HOURS:g} h settle time), skipped")
                continue
            if not common.has_files(spark, common.path(common.RAW, "estimated_timetable", f"date={d}")):
                print(f"--- bronze {d}: no raw estimated_timetable files, skipped")
                continue
            try:
                step("bronze", d, _stage("bronze"))
            finally:
                # free the scratch disk: a backfill of many days would otherwise fill it up
                common.drop_cache(common.RAW, "date", d)
                common.drop_cache(common.BRONZE, "date", d)

    def silver_day(d):
        nxt = common.shift(d, 1)
        if "clean" in a.stages and not skip("clean", d):
            if not common.is_done(spark, "bronze", d):
                return
            if not common.is_done(spark, "bronze", nxt):
                print(f"--- clean {d}: waiting for bronze {nxt} (late trips end after midnight)")
                return
            delay = float(os.getenv("SILVER_DELAY_HOURS") or 0)
            if delay:
                # silver of D runs only SILVER_DELAY_HOURS after the last bronze it needs (D+1) finished
                age = common.done_age_hours(spark, "bronze", nxt)
                if age is None or age < delay:
                    print(f"--- clean {d}: bronze {nxt} finished {age or 0:.1f} h ago, "
                          f"silver waits until {delay:g} h")
                    return
            if not step("clean", d, _stage("silver")):
                return
        if "stop_analysis" in a.stages and not skip("stop_analysis", d) \
                and common.is_done(spark, "clean", d):
            step("stop_analysis", d, _stage("stop_analysis"))

    if "clean" in a.stages or "stop_analysis" in a.stages:
        for d in all_days:
            try:
                silver_day(d)
            finally:
                # bronze up to D-1 is not needed any more (day D+1 reads D .. D+2)
                common.drop_cache(common.BRONZE, "date", common.shift(d, -1))
                common.drop_cache(common.SILVER, "service_date", d)

    print(f"=== finished, failed: {failed or 'none'}", flush=True)
    spark.stop()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
