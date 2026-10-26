"""Inspect raw bronze files from the static timetable (GTFS zip) and the
estimated timetable (SIRI ET XML). Read-only: no input file is changed.

Usage (from the repo root on the VM):
    python3 scripts/inspect_timetables.py                     # newest file of each feed
    python3 scripts/inspect_timetables.py --gtfs path/to.zip --et path/to.xml
    python3 scripts/inspect_timetables.py --et path/to.xml.gz  # gzipped files work too

Only the Python standard library is used.
"""
import argparse
import csv
import gzip
import io
import statistics
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

DATA_DIR = Path("data")

# A departure counts as "on time" if it is at most 1 min early and 3 min late.
ON_TIME_EARLY = -60
ON_TIME_LATE = 180


# --------------------------------------------------------------------- helpers
def newest(feed, patterns):
    files = [p for pat in patterns for p in (DATA_DIR / feed).glob(f"date=*/{pat}")]
    return max(files, key=lambda p: p.name) if files else None


def local(tag):
    return tag.rsplit("}", 1)[-1]


def child_text(element, name):
    for child in element:
        if local(child.tag) == name:
            return (child.text or "").strip()
    return None


def parse_time(value):
    if not value:
        return None
    # Python < 3.11 cannot parse more than 6 decimals, SIRI sometimes has 9
    if "." in value:
        head, rest = value.split(".", 1)
        digits = ""
        while rest and rest[0].isdigit():
            digits, rest = digits + rest[0], rest[1:]
        value = f"{head}.{digits[:6]}{rest}"
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def seconds_between(later, earlier):
    a, b = parse_time(later), parse_time(earlier)
    return (a - b).total_seconds() if a and b else None


def fmt_delay(seconds):
    sign = "-" if seconds < 0 else "+"
    seconds = abs(int(round(seconds)))
    return f"{sign}{seconds // 60}:{seconds % 60:02d}"


def header(title):
    print("\n" + "=" * 65)
    print(title)
    print("=" * 65)


def read_csv(zf, name):
    """Stream rows from one GTFS file inside the zip."""
    with zf.open(name) as raw:
        yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))


# ------------------------------------------------------------- static (GTFS)
ROUTE_TYPES = {"0": "tram", "1": "metro", "2": "rail", "3": "bus", "4": "ferry",
               "700": "bus", "715": "bus (on demand)", "1000": "water", "1200": "ferry",
               "1100": "air"}


def inspect_gtfs(path):
    header(f"STATIC TIMETABLE (GTFS)  {path}")
    print(f"File size: {path.stat().st_size / 1e6:.1f} MB")

    lookup = {"routes": {}, "stops": set(), "trips": set()}
    with zipfile.ZipFile(path) as zf:
        names = sorted(n for n in zf.namelist() if n.endswith(".txt"))

        print("\nFILES IN THE ZIP (rows, columns):")
        for name in names:
            with zf.open(name) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8-sig")
                columns = text.readline().strip().split(",")
                rows = sum(1 for _ in text)
            print(f"  {name:<22} {rows:>9,} rows   {', '.join(columns)}")

        # Routes
        routes = list(read_csv(zf, "routes.txt")) if "routes.txt" in names else []
        types = Counter(ROUTE_TYPES.get(r.get("route_type", ""), r.get("route_type")) for r in routes)
        for r in routes:
            lookup["routes"][r["route_id"]] = r.get("route_short_name") or r.get("route_long_name") or r["route_id"]
        print(f"\nROUTES: {len(routes)}")
        for kind, count in types.most_common():
            print(f"  {kind}: {count}")

        # Trips per route
        trips_per_route = Counter()
        services = set()
        if "trips.txt" in names:
            for t in read_csv(zf, "trips.txt"):
                trips_per_route[t["route_id"]] += 1
                services.add(t.get("service_id"))
                lookup["trips"].add(t["trip_id"])
        print(f"\nTRIPS: {sum(trips_per_route.values()):,}  (service patterns: {len(services)})")
        print("Routes with the most trips in the feed:")
        for route_id, count in trips_per_route.most_common(10):
            print(f"  {lookup['routes'].get(route_id, route_id):<10} {route_id:<22} {count:,} trips")

        # Stops
        if "stops.txt" in names:
            kinds = Counter()
            for s in read_csv(zf, "stops.txt"):
                lookup["stops"].add(s["stop_id"])
                kinds["station (parent)" if s.get("location_type") == "1" else "quay / stop"] += 1
            print(f"\nSTOPS: {len(lookup['stops']):,}")
            for kind, count in kinds.most_common():
                print(f"  {kind}: {count:,}")

        # Stop times: how long are trips?
        if "stop_times.txt" in names:
            stops_per_trip = Counter(row["trip_id"] for row in read_csv(zf, "stop_times.txt"))
            values = list(stops_per_trip.values())
            print(f"\nSTOP TIMES: {sum(values):,} scheduled stop visits")
            if values:
                print(f"  stops per trip: min {min(values)}, median {statistics.median(values):.0f}, max {max(values)}")

        # Which dates does the feed cover?
        dates = set()
        if "calendar_dates.txt" in names:
            dates |= {row["date"] for row in read_csv(zf, "calendar_dates.txt")
                      if row.get("exception_type", "1") == "1"}
        if "calendar.txt" in names:
            for row in read_csv(zf, "calendar.txt"):
                dates |= {row["start_date"], row["end_date"]}
        if dates:
            print(f"\nSERVICE DATES: {min(dates)} -> {max(dates)}  ({len(dates)} distinct dates listed)")

        if "feed_info.txt" in names:
            for row in read_csv(zf, "feed_info.txt"):
                print("\nFEED INFO:", {k: v for k, v in row.items() if v})

    return lookup


# --------------------------------------------------- estimated timetable (ET)
def open_xml(path):
    return gzip.open(path, "rb") if path.suffix == ".gz" else path.open("rb")


def call_delay(call, recorded):
    """Delay of one stop visit in seconds: actual (recorded) or expected minus aimed.
    Departure is used when there is one, otherwise arrival (last stop)."""
    prefix = "Actual" if recorded else "Expected"
    for kind in ("Departure", "Arrival"):
        delay = seconds_between(child_text(call, f"{prefix}{kind}Time"),
                                child_text(call, f"Aimed{kind}Time"))
        if delay is not None:
            return delay
    return None


def inspect_et(path, gtfs=None):
    header(f"ESTIMATED TIMETABLE (SIRI ET)  {path}")
    print(f"File size: {path.stat().st_size / 1e6:.1f} MB")

    routes = gtfs["routes"] if gtfs else {}
    journeys = 0
    journey_ids = Counter()
    modes, monitored = Counter(), Counter()
    cancelled_journeys = 0
    call_counts = Counter()
    statuses = Counter()
    cancelled_calls = inaccurate = 0
    recorded_at = []
    more_data = None
    delays_actual, delays_expected = [], []
    per_line = defaultdict(list)          # line -> actual delays
    worst = []                            # (delay, line, stop, aimed)
    line_refs, stop_refs = set(), set()

    with open_xml(path) as f:
        for event, elem in ET.iterparse(f, events=("end",)):
            tag = local(elem.tag)
            if tag == "MoreData":
                more_data = (elem.text or "").strip()
            if tag != "EstimatedVehicleJourney":
                continue

            journeys += 1
            line = child_text(elem, "LineRef") or "?"
            line_refs.add(line)
            label = routes.get(line, line.replace("KOL:Line:", ""))
            modes[child_text(elem, "VehicleMode") or "unknown"] += 1
            monitored[child_text(elem, "Monitored") or "missing"] += 1
            if child_text(elem, "Cancellation") == "true":
                cancelled_journeys += 1
            if child_text(elem, "RecordedAtTime"):
                recorded_at.append(child_text(elem, "RecordedAtTime"))
            for ref in elem.iter():
                if local(ref.tag) == "DatedVehicleJourneyRef":
                    journey_ids[ref.text] += 1
                    break

            for call in elem.iter():
                ctag = local(call.tag)
                if ctag not in ("RecordedCall", "EstimatedCall"):
                    continue
                recorded = ctag == "RecordedCall"
                call_counts[ctag] += 1
                stop = child_text(call, "StopPointName") or child_text(call, "StopPointRef") or "?"
                stop_refs.add(child_text(call, "StopPointRef"))
                statuses[child_text(call, "DepartureStatus") or child_text(call, "ArrivalStatus") or "none"] += 1
                if child_text(call, "Cancellation") == "true":
                    cancelled_calls += 1
                    continue
                if child_text(call, "PredictionInaccurate") == "true":
                    inaccurate += 1
                delay = call_delay(call, recorded)
                if delay is None:
                    continue
                if recorded:
                    delays_actual.append(delay)
                    per_line[label].append(delay)
                    aimed = child_text(call, "AimedDepartureTime") or child_text(call, "AimedArrivalTime")
                    worst.append((delay, label, stop, aimed))
                else:
                    delays_expected.append(delay)
            elem.clear()  # keep memory low on big files

    # ---- overview
    print(f"\nJourneys in snapshot: {journeys:,}  (unique: {len(journey_ids):,})")
    if more_data == "true":
        print("  WARNING: MoreData=true, the snapshot was cut off (raise SIRI_MAX_SIZE)")
    print("By mode:     ", dict(modes.most_common()))
    print("Monitored:   ", dict(monitored), " (false = no live tracking, only the plan)")
    print(f"Cancelled journeys: {cancelled_journeys:,}")
    if recorded_at:
        print(f"RecordedAtTime range: {min(recorded_at)[:19]} -> {max(recorded_at)[:19]}")

    total_calls = sum(call_counts.values())
    print(f"\nStop visits (calls): {total_calls:,}")
    print(f"  RecordedCall  (already happened, actual times): {call_counts['RecordedCall']:,}")
    print(f"  EstimatedCall (still to come, predicted times):  {call_counts['EstimatedCall']:,}")
    print(f"  Cancelled stop visits: {cancelled_calls:,}")
    print(f"  Marked PredictionInaccurate: {inaccurate:,}")
    print("  Status values:", dict(statuses.most_common()))

    # ---- delays
    def summary(name, values):
        if not values:
            print(f"\n{name}: no data")
            return
        on_time = sum(ON_TIME_EARLY <= d <= ON_TIME_LATE for d in values) / len(values)
        early = sum(d < ON_TIME_EARLY for d in values) / len(values)
        late = sum(d > ON_TIME_LATE for d in values) / len(values)
        print(f"\n{name} ({len(values):,} stop visits):")
        print(f"  mean {fmt_delay(statistics.mean(values))}, median {fmt_delay(statistics.median(values))}, "
              f"worst {fmt_delay(max(values))}, earliest {fmt_delay(min(values))}")
        print(f"  on time {on_time:.0%}, early {early:.0%}, late {late:.0%}  "
              f"(on time = {ON_TIME_EARLY // 60} to +{ON_TIME_LATE // 60} min)")

    summary("ACTUAL DELAY (RecordedCalls)", delays_actual)
    summary("PREDICTED DELAY (EstimatedCalls)", delays_expected)

    if per_line:
        print("\nLINES BY MEAN ACTUAL DELAY (at least 5 stop visits):")
        print(f"  {'line':<12}{'visits':>7}{'mean':>9}{'median':>9}{'on time':>9}")
        ranked = sorted(((statistics.mean(v), k, v) for k, v in per_line.items() if len(v) >= 5), reverse=True)
        for mean, line, values in ranked[:15]:
            on_time = sum(ON_TIME_EARLY <= d <= ON_TIME_LATE for d in values) / len(values)
            print(f"  {line:<12}{len(values):>7}{fmt_delay(mean):>9}"
                  f"{fmt_delay(statistics.median(values)):>9}{on_time:>9.0%}")

    if worst:
        print("\nMOST DELAYED STOP VISITS (actual):")
        for delay, line, stop, aimed in sorted(worst, reverse=True)[:10]:
            print(f"  {fmt_delay(delay):>7}  line {line:<8} {stop:<30} planned {(aimed or '')[11:16]}")

    # ---- can ET be joined to GTFS?
    if gtfs:
        def coverage(ids, known):
            ids = {i for i in ids if i}
            return f"{len(ids & known)}/{len(ids)} ({len(ids & known) / len(ids):.0%})" if ids else "n/a"
        print("\nLINK TO GTFS (IDs from ET found in the static timetable):")
        print("  LineRef               -> routes.route_id:", coverage(line_refs, set(routes)))
        print("  StopPointRef          -> stops.stop_id:  ", coverage(stop_refs, gtfs["stops"]))
        print("  DatedVehicleJourneyRef-> trips.trip_id:  ", coverage(set(journey_ids), gtfs["trips"]))


# ------------------------------------------------------------------------ main
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gtfs", type=Path, help="static_timetable .zip (default: newest in data/)")
    parser.add_argument("--et", type=Path, help="estimated_timetable .xml or .xml.gz (default: newest in data/)")
    args = parser.parse_args()

    gtfs_path = args.gtfs or newest("static_timetable", ["*.zip"])
    et_path = args.et or newest("estimated_timetable", ["*.xml", "*.xml.gz"])

    gtfs = None
    if gtfs_path:
        gtfs = inspect_gtfs(gtfs_path)
    else:
        print("No static_timetable file found (use --gtfs).")

    if et_path:
        inspect_et(et_path, gtfs)
    else:
        print("No estimated_timetable file found (use --et).")

    print("\nDone. No input files were modified.")


if __name__ == "__main__":
    main()
