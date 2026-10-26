"""Convert raw bronze files to readable JSON, in the same style as the
vehicle_positions files. Read-only: input files are never changed.

  estimated timetable (SIRI ET .xml)  ->  one object per journey, with its stop visits
  static timetable    (GTFS .zip)     ->  routes.json and stops.json

Usage (from the repo root on the VM):
    python3 scripts/export_json.py                       # newest file of each feed -> output/
    python3 scripts/export_json.py --line 8              # only line 8 (public number)
    python3 scripts/export_json.py --limit 20            # only the first 20 journeys
    python3 scripts/export_json.py --et path.xml[.gz] --gtfs path.zip --out myfolder

Only the Python standard library is used.
"""
import argparse
import csv
import gzip
import io
import json
import zipfile
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

DATA_DIR = Path("data")
ROUTE_TYPES = {"0": "tram", "1": "metro", "2": "rail", "3": "bus", "4": "ferry", "700": "bus",
               "715": "bus", "1000": "water", "1100": "air", "1200": "ferry"}


# --------------------------------------------------------------------- helpers
def newest(feed, patterns):
    files = [p for pat in patterns for p in (DATA_DIR / feed).glob(f"date=*/{pat}")]
    return max(files, key=lambda p: p.name) if files else None


def local(tag):
    return tag.rsplit("}", 1)[-1]


def text(element, name):
    for child in element:
        if local(child.tag) == name:
            return (child.text or "").strip() or None
    return None


def parse_time(value):
    if not value:
        return None
    if "." in value:  # trim nanoseconds to microseconds
        head, rest = value.split(".", 1)
        digits = ""
        while rest and rest[0].isdigit():
            digits, rest = digits + rest[0], rest[1:]
        value = f"{head}.{digits[:6]}{rest}"
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def delay_seconds(actual, aimed):
    a, b = parse_time(actual), parse_time(aimed)
    return round((a - b).total_seconds()) if a and b else None


def read_csv(zf, name):
    with zf.open(name) as raw:
        yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))


class JsonArrayWriter:
    """Writes a JSON array one item at a time, so big files never sit in memory."""

    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path, self.f, self.count = path, path.open("w", encoding="utf-8"), 0
        self.f.write("[\n")

    def write(self, item):
        body = json.dumps(item, indent=2, ensure_ascii=False)
        self.f.write((",\n" if self.count else "") + "\n".join("  " + l for l in body.splitlines()))
        self.count += 1

    def close(self):
        self.f.write("\n]\n")
        self.f.close()
        print(f"  wrote {self.count:,} records -> {self.path}")


# ------------------------------------------------------------- static (GTFS)
def export_gtfs(path, out_dir):
    print(f"GTFS: {path}")
    routes = {}
    with zipfile.ZipFile(path) as zf:
        names = set(zf.namelist())

        agencies = {a["agency_id"]: a.get("agency_name") for a in read_csv(zf, "agency.txt")} \
            if "agency.txt" in names else {}
        trips = {}
        if "trips.txt" in names:
            for t in read_csv(zf, "trips.txt"):
                trips[t["route_id"]] = trips.get(t["route_id"], 0) + 1

        writer = JsonArrayWriter(out_dir / "routes.json")
        for r in read_csv(zf, "routes.txt"):
            record = {
                "lineRef": r["route_id"],
                "publicCode": r.get("route_short_name") or None,
                "lineName": r.get("route_long_name") or None,
                "mode": ROUTE_TYPES.get(r.get("route_type"), r.get("route_type")),
                "operator": agencies.get(r.get("agency_id"), r.get("agency_id")),
                "trips": trips.get(r["route_id"], 0),
            }
            routes[r["route_id"]] = record
            writer.write(record)
        writer.close()

        if "stops.txt" in names:
            writer = JsonArrayWriter(out_dir / "stops.json")
            for s in read_csv(zf, "stops.txt"):
                writer.write({
                    "stopRef": s["stop_id"],
                    "name": s.get("stop_name"),
                    "type": "station" if s.get("location_type") == "1" else "quay",
                    "parentStation": s.get("parent_station") or None,
                    "location": {
                        "latitude": float(s["stop_lat"]) if s.get("stop_lat") else None,
                        "longitude": float(s["stop_lon"]) if s.get("stop_lon") else None,
                    },
                })
            writer.close()
    return routes


# --------------------------------------------------- estimated timetable (ET)
def call_record(call, recorded):
    kind = "Actual" if recorded else "Expected"
    aimed_arr, aimed_dep = text(call, "AimedArrivalTime"), text(call, "AimedDepartureTime")
    arr, dep = text(call, f"{kind}ArrivalTime"), text(call, f"{kind}DepartureTime")
    delay = delay_seconds(dep, aimed_dep)
    if delay is None:
        delay = delay_seconds(arr, aimed_arr)
    return {
        "order": int(text(call, "Order")) if text(call, "Order") else None,
        "stopRef": text(call, "StopPointRef"),
        "stopName": text(call, "StopPointName"),
        "type": "actual" if recorded else "expected",
        "aimedArrival": aimed_arr,
        "arrival": arr,
        "aimedDeparture": aimed_dep,
        "departure": dep,
        "delay": delay,
        "status": text(call, "DepartureStatus") or text(call, "ArrivalStatus"),
        "cancelled": text(call, "Cancellation") == "true",
    }


def export_et(path, out_dir, routes, line_filter, limit):
    print(f"Estimated timetable: {path}")
    out = out_dir / (path.name.split(".")[0] + ".json")
    writer = JsonArrayWriter(out)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as f:
        for _, elem in ET.iterparse(f, events=("end",)):
            if local(elem.tag) != "EstimatedVehicleJourney":
                continue
            line_ref = text(elem, "LineRef")
            route = routes.get(line_ref, {})
            public_code = route.get("publicCode")
            if line_filter and line_filter not in (public_code, line_ref):
                elem.clear()
                continue

            calls = []
            for call in elem.iter():
                tag = local(call.tag)
                if tag in ("RecordedCall", "EstimatedCall"):
                    calls.append(call_record(call, tag == "RecordedCall"))
            calls.sort(key=lambda c: c["order"] or 0)

            journey_ref = date = None
            for child in elem.iter():
                if local(child.tag) == "DatedVehicleJourneyRef":
                    journey_ref = child.text
                elif local(child.tag) == "DataFrameRef":
                    date = child.text

            writer.write({
                "journeyRef": journey_ref,
                "date": date,
                "recordedAt": text(elem, "RecordedAtTime"),
                "line": {
                    "lineRef": line_ref,
                    "lineName": route.get("lineName"),
                    "publicCode": public_code,
                },
                "mode": text(elem, "VehicleMode"),
                "direction": text(elem, "DirectionRef"),
                "operatorRef": text(elem, "OperatorRef"),
                "origin": text(elem, "OriginName") or (calls[0]["stopName"] if calls else None),
                "destination": next((text(c, "DestinationDisplay") for c in elem.iter()
                                     if local(c.tag) in ("RecordedCall", "EstimatedCall")
                                     and text(c, "DestinationDisplay")), None),
                "monitored": text(elem, "Monitored") == "true",
                "cancelled": text(elem, "Cancellation") == "true",
                "calls": calls,
            })
            elem.clear()
            if limit and writer.count >= limit:
                break
    writer.close()


# ------------------------------------------------------------------------ main
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--gtfs", type=Path, help="static_timetable .zip (default: newest in data/)")
    parser.add_argument("--et", type=Path, help="estimated_timetable .xml or .xml.gz (default: newest in data/)")
    parser.add_argument("--out", type=Path, default=Path("output"), help="output folder (default: output/)")
    parser.add_argument("--line", help="only this line, e.g. 8 or KOL:Line:8_1011")
    parser.add_argument("--limit", type=int, help="max number of journeys to write")
    args = parser.parse_args()

    gtfs_path = args.gtfs or newest("static_timetable", ["*.zip"])
    et_path = args.et or newest("estimated_timetable", ["*.xml", "*.xml.gz"])

    routes = {}
    if gtfs_path:
        routes = export_gtfs(gtfs_path, args.out)
    else:
        print("No static_timetable file found: line numbers will be missing (use --gtfs).")

    if et_path:
        export_et(et_path, args.out, routes, args.line, args.limit)
    else:
        print("No estimated_timetable file found (use --et).")


if __name__ == "__main__":
    main()
