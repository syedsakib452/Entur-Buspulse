"""Parsers for the raw Entur files: one raw file (bytes) in, table rows (dicts) out.

The same parsing rules as tables/build_bronze_tables.py, so the bronze tables keep
their columns. Pure Python (no pandas or pyarrow): Spark calls these inside its
worker processes, one file at a time, so a whole day is never held in memory.
"""
import io
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone


def snapshot_time(name):
    match = re.search(r"_(\d{8}T\d{6})Z", name)
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc) if match else None


def local(tag):
    return tag.rsplit("}", 1)[-1]


def text(element, name):
    for child in element:
        if local(child.tag) == name:
            return (child.text or "").strip() or None
    return None


def first_text(element, name):
    """First matching element anywhere below `element`."""
    for child in element.iter():
        if local(child.tag) == name:
            return (child.text or "").strip() or None
    return None


def flag(value):
    return None if value is None else value == "true"


def num(value):
    return None if value is None else float(value)


# ------------------------------------------------------------------- parsers
# Each parser gets the raw bytes of one file and yields table rows (dicts).

def parse_vehicle_positions(payload, base):
    for v in json.loads(payload).get("data", {}).get("vehicles") or []:
        line, loc = v.get("line") or {}, v.get("location") or {}
        yield {**base,
               "vehicle_id": v.get("vehicleId"), "last_updated": v.get("lastUpdated"),
               "line_ref": line.get("lineRef"), "line_name": line.get("lineName"),
               "public_code": line.get("publicCode"),
               "latitude": num(loc.get("latitude")), "longitude": num(loc.get("longitude")),
               "bearing": num(v.get("bearing")), "speed": num(v.get("speed")),
               "delay": num(v.get("delay")), "occupancy_status": v.get("occupancyStatus")}


def parse_service_alerts(payload, base):
    for _, elem in ET.iterparse(io.BytesIO(payload)):
        if local(elem.tag) != "PtSituationElement":
            continue
        alert = {**base,
                 "situation_number": text(elem, "SituationNumber"),
                 "creation_time": text(elem, "CreationTime"),
                 "version_time": text(elem, "VersionedAtTime"),
                 "progress": text(elem, "Progress"), "severity": text(elem, "Severity"),
                 "report_type": text(elem, "ReportType"), "planned": text(elem, "Planned"),
                 "summary": text(elem, "Summary"),
                 "valid_start": first_text(elem, "StartTime"), "valid_end": first_text(elem, "EndTime")}
        empty = {"affect_type": None, "line_ref": None, "journey_ref": None,
                 "operator_ref": None, "origin_aimed_departure": None}
        affected = []
        for part in elem.iter():
            if local(part.tag) == "AffectedVehicleJourney":
                affected.append({**empty, "affect_type": "journey",
                                 "line_ref": first_text(part, "LineRef"),
                                 "journey_ref": first_text(part, "VehicleJourneyRef"),
                                 "operator_ref": first_text(part, "OperatorRef"),
                                 "origin_aimed_departure": first_text(part, "OriginAimedDepartureTime")})
            elif local(part.tag) == "AffectedNetwork":
                operator = first_text(part, "OperatorRef")
                lines = [first_text(l, "LineRef") for l in part.iter() if local(l.tag) == "AffectedLine"]
                for line_ref in lines or [None]:
                    affected.append({**empty, "affect_type": "line", "line_ref": line_ref,
                                     "operator_ref": operator})
        for a in affected or [empty]:   # an alert without affects still gets one row
            yield {**alert, **a}
        elem.clear()


def parse_estimated_timetable(payload, base):
    for _, elem in ET.iterparse(io.BytesIO(payload)):
        if local(elem.tag) != "EstimatedVehicleJourney":
            continue
        journey = {**base,
                   "recorded_at": text(elem, "RecordedAtTime"),
                   "journey_ref": first_text(elem, "DatedVehicleJourneyRef"),
                   "data_frame_ref": first_text(elem, "DataFrameRef"),
                   "line_ref": text(elem, "LineRef"), "direction_ref": text(elem, "DirectionRef"),
                   "vehicle_mode": text(elem, "VehicleMode"), "operator_ref": text(elem, "OperatorRef"),
                   "monitored": flag(text(elem, "Monitored")),
                   "journey_cancelled": flag(text(elem, "Cancellation"))}
        for call in elem.iter():
            kind = local(call.tag)
            if kind not in ("RecordedCall", "EstimatedCall"):
                continue
            order = text(call, "Order")
            yield {**journey,
                   "call_type": "recorded" if kind == "RecordedCall" else "estimated",
                   "stop_order": int(order) if order else None,
                   "stop_ref": text(call, "StopPointRef"), "stop_name": text(call, "StopPointName"),
                   "destination": text(call, "DestinationDisplay"),
                   "aimed_arrival": text(call, "AimedArrivalTime"),
                   "expected_arrival": text(call, "ExpectedArrivalTime"),
                   "actual_arrival": text(call, "ActualArrivalTime"),
                   "aimed_departure": text(call, "AimedDepartureTime"),
                   "expected_departure": text(call, "ExpectedDepartureTime"),
                   "actual_departure": text(call, "ActualDepartureTime"),
                   "arrival_status": text(call, "ArrivalStatus"),
                   "departure_status": text(call, "DepartureStatus"),
                   "call_cancelled": flag(text(call, "Cancellation")),
                   "prediction_inaccurate": flag(text(call, "PredictionInaccurate"))}
        elem.clear()


# raw feed folder -> (bronze table, parser)
FEEDS = {"vehicle_positions": ("vehicle_positions", parse_vehicle_positions),
         "service_alerts": ("service_alerts", parse_service_alerts),
         "estimated_timetable": ("stop_visits", parse_estimated_timetable)}
