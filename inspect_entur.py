
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

VEHICLE_FILE = Path(
    "data/vehicle_positions/date=2026-10-20/"
    "vehicle_positions_20261020T144813Z.json"
)

ALERT_FILE = Path(
    "data/service_alerts/date=2026-10-20/"
    "service_alerts_20261020T145414Z.xml"
)


def local(tag):
    return tag.rsplit("}", 1)[-1]


def get_all_xml(element, name):
    return [
        item.text.strip()
        for item in element.iter()
        if local(item.tag) == name and item.text
    ]


def find_route_values(obj):
    """Find likely route/line fields without discarding source data."""
    found = []

    def walk(value, path=""):
        if isinstance(value, dict):
            for key, child in value.items():
                current = f"{path}.{key}" if path else key
                if any(word in key.lower() for word in
                       ("line", "route", "publishedlinename")):
                    if isinstance(child, (str, int, float)):
                        found.append((current, str(child)))
                walk(child, current)
        elif isinstance(value, list):
            for i, child in enumerate(value):
                walk(child, f"{path}[{i}]")

    walk(obj)
    return found


def route_number(value):
    # Handles references such as KOL:Line:8_1013 and plain line numbers.
    match = re.search(r":Line:(\d+)", value, re.I)
    if match:
        return match.group(1)

    match = re.fullmatch(r"\s*(\d+)\s*", value)
    if match:
        return match.group(1)

    return None


def main():
    # VEHICLE POSITIONS
    print("\n" + "=" * 65)
    print("VEHICLE POSITIONS JSON")
    print("=" * 65)

    with VEHICLE_FILE.open(encoding="utf-8") as f:
        data = json.load(f)

    print("Top-level structure:", type(data).__name__)
    if isinstance(data, dict):
        print("Top-level keys:", list(data.keys()))

    vehicles = data.get("data", {}).get("vehicles", [])
    if not isinstance(vehicles, list):
        print("Could not find a list at data.vehicles.")
        print("Inspect the JSON structure above.")
        return

    print("Total vehicle records:", len(vehicles))

    vehicle_ids = set()
    route_counts = Counter()
    route_examples = {}
    unidentified = 0

    for vehicle in vehicles:
        if not isinstance(vehicle, dict):
            unidentified += 1
            continue

        # Report actual ID field names and count unique IDs.
        for key in ("vehicleId", "vehicleRef", "VehicleRef", "id"):
            if vehicle.get(key) not in (None, ""):
                vehicle_ids.add(str(vehicle[key]))
                break

        candidates = find_route_values(vehicle)
        routes = {
            route_number(value)
            for _, value in candidates
            if route_number(value) is not None
        }

        if not routes:
            unidentified += 1
            continue

        for route in routes:
            route_counts[route] += 1
            route_examples.setdefault(route, candidates)

    print("Unique vehicle IDs found:", len(vehicle_ids))
    print("Records without a recognizable numeric route:", unidentified)
    print("\nROUTES FOUND IN VEHICLE JSON:")

    for route, count in sorted(
        route_counts.items(), key=lambda item: int(item[0])
    ):
        print(f"  Route {route}: {count} vehicle records")
        print("    Example route fields:", route_examples[route][:8])

    if vehicles:
        print("\nFIELDS IN FIRST VEHICLE:")
        print(json.dumps(vehicles[0], indent=2, ensure_ascii=False))

    # SERVICE ALERTS
    print("\n" + "=" * 65)
    print("SERVICE ALERTS XML")
    print("=" * 65)

    root = ET.parse(ALERT_FILE).getroot()
    alerts = [
        element for element in root.iter()
        if local(element.tag) == "PtSituationElement"
    ]

    print("Total alert records:", len(alerts))

    alert_routes = Counter()
    alert_journeys = Counter()
    alert_summaries = Counter()

    for alert in alerts:
        lines = get_all_xml(alert, "LineRef")
        journeys = get_all_xml(alert, "VehicleJourneyRef")
        summaries = get_all_xml(alert, "Summary")

        for line in lines:
            match = re.search(r":Line:(\d+)", line, re.I)
            route = match.group(1) if match else None
            alert_routes[route or line] += 1

        for journey in journeys:
            alert_journeys[journey] += 1

        for summary in summaries:
            alert_summaries[summary] += 1

    print("\nROUTES/LINES IN SERVICE ALERTS:")
    for route, count in sorted(alert_routes.items()):
        print(f"  {route}: {count} affected-journey references")

    print("\nUnique affected journey references:", len(alert_journeys))

    print("\nALERT REASONS:")
    for summary, count in alert_summaries.most_common():
        print(f"  {count}x {summary}")

    print("\nDiagnostic complete. No input files were modified.")


if __name__ == "__main__":
    main()
