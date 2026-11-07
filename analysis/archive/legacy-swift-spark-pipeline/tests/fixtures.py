"""A small but realistic day of raw files, in the formats the collectors upload, for the Spark tests.

Line L1 (bus, route_type 700): trips T1 (bus A) and T2 (bus B), 15 stops 400 m apart (tests/sim.py),
plus T3, scheduled but never in the feed. Line F1 (ferry, route_type 1200): FT1 with vehicle_mode
"ferry" and FT2 without a vehicle_mode (must still be removed, by route_type).
Snapshots every 2 minutes; T1's newest snapshot lacks one actual time (must be rescued from an older
one); T2's last calls only get actual times in a snapshot on the next UTC day.
"""
import gzip
import io
import json
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from spark_jobs.tests.sim import N_STOPS, STOP_EVERY_M, line_day

DAY = "2026-10-30"
NEXT = "2026-10-31"
LOCAL = timezone(timedelta(hours=1))                    # Norway after 25 Oct = UTC+1
T0 = datetime(2026, 10, 30, 7, 0, tzinfo=timezone.utc).timestamp()   # 08:00 local
LAT0, LON0, M_LAT = 58.97, 5.73, 111_320.0
M_LON = M_LAT * np.cos(np.radians(LAT0))
SCHED_STEP = 50                                          # timetable: 50 s per stop
NS = "http://www.siri.org.uk/siri"


def latlon(x, y):
    return LAT0 + y / M_LAT, LON0 + x / M_LON


def iso_local(t):
    return datetime.fromtimestamp(t, LOCAL).isoformat(timespec="milliseconds")


def iso_z(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def hms_local(t):
    d = datetime.fromtimestamp(t, LOCAL)
    return f"{d.hour:02d}:{d.minute:02d}:{d.second:02d}"


def put(root, bucket, rel, data):
    p = Path(root) / bucket / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


def journeys(dep):
    """Timetable of the trips: aimed departure per stop (epoch s), and the feed's actual times."""
    start = {"T1": T0, "T2": T0 + 600, "T3": T0 + 1200, "FT1": T0 + 300, "FT2": T0 + 900}
    out = {}
    for trip, t_start in start.items():
        n = N_STOPS if trip.startswith("T") else 2
        aimed = [t_start + i * SCHED_STEP for i in range(n)]
        actual = [dep[trip][i] for i in range(n)] if trip in dep else [a + 30 for a in aimed]
        out[trip] = (aimed, actual)
    return out


def et_snapshot(snap_t, trips, drop_actual=None):
    """One SIRI ET response at snap_t. Calls at or before snap_t are RecordedCalls with actual times."""
    parts = [f'<?xml version="1.0" encoding="UTF-8"?><Siri xmlns="{NS}" version="2.0"><ServiceDelivery>'
             f"<EstimatedTimetableDelivery><EstimatedJourneyVersionFrame>"]
    for trip, (aimed, actual) in trips.items():
        if trip == "T3":
            continue                                    # never in the feed
        line = "F1" if trip.startswith("F") else "L1"
        mode = {"FT1": "<VehicleMode>ferry</VehicleMode>", "FT2": ""}.get(trip, "<VehicleMode>bus</VehicleMode>")
        rec, est = [], []
        for i, (a, act) in enumerate(zip(aimed, actual)):
            stop = f"FQ{i}" if trip.startswith("F") else f"S{i}"
            head = f"<StopPointRef>{stop}</StopPointRef><Order>{i + 1}</Order>"
            if act <= snap_t:
                acts = "" if (trip, i) == drop_actual else (
                    f"<ActualArrivalTime>{iso_local(act)}</ActualArrivalTime>"
                    f"<ActualDepartureTime>{iso_local(act)}</ActualDepartureTime>")
                rec.append(f"<RecordedCall>{head}<AimedArrivalTime>{iso_local(a)}</AimedArrivalTime>"
                           f"<AimedDepartureTime>{iso_local(a)}</AimedDepartureTime>{acts}</RecordedCall>")
            else:
                est.append(f"<EstimatedCall>{head}<AimedArrivalTime>{iso_local(a)}</AimedArrivalTime>"
                           f"<ExpectedArrivalTime>{iso_local(a + 20)}</ExpectedArrivalTime>"
                           f"<AimedDepartureTime>{iso_local(a)}</AimedDepartureTime>"
                           f"<ExpectedDepartureTime>{iso_local(a + 20)}</ExpectedDepartureTime>"
                           f"<DepartureStatus>delayed</DepartureStatus></EstimatedCall>")
        parts.append(
            f"<EstimatedVehicleJourney><RecordedAtTime>{iso_local(snap_t)}</RecordedAtTime>"
            f"<LineRef>{line}</LineRef><DirectionRef>outbound</DirectionRef>"
            f"<FramedVehicleJourneyRef><DataFrameRef>{DAY}</DataFrameRef>"
            f"<DatedVehicleJourneyRef>{trip}</DatedVehicleJourneyRef></FramedVehicleJourneyRef>"
            f"{mode}<OperatorRef>OP1</OperatorRef><Monitored>true</Monitored>"
            f"<RecordedCalls>{''.join(rec)}</RecordedCalls><EstimatedCalls>{''.join(est)}</EstimatedCalls>"
            f"</EstimatedVehicleJourney>")
    parts.append("</EstimatedJourneyVersionFrame></EstimatedTimetableDelivery></ServiceDelivery></Siri>")
    return "".join(parts).encode()


def gtfs_zip(trips, shape_pts):
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nKOL,Kolumbus,https://x,Europe/Oslo\n",
        "routes.txt": "route_id,agency_id,route_short_name,route_long_name,route_type\n"
                      "L1,KOL,1,Line one,700\nF1,KOL,1054,Ferry one,1200\n",
        "trips.txt": "﻿route_id,service_id,trip_id,shape_id\n"          # a byte-order mark, as real files have
                     "L1,S1,T1,SH1\nL1,S1,T2,SH1\nL1,S1,T3,SH1\nF1,S1,FT1,\nF1,S1,FT2,\n",
        "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
                        "S1,1,1,1,1,1,1,1,20261001,20261231\nS9,0,0,0,0,0,0,0,20261001,20261231\n",
        "calendar_dates.txt": "service_id,date,exception_type\nS9,20261101,1\n",
    }
    st = ["trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type"]
    for trip, (aimed, _) in trips.items():
        for i, a in enumerate(aimed):
            stop = f"FQ{i}" if trip.startswith("F") else f"S{i}"
            st.append(f"{trip},{hms_local(a)},{hms_local(a)},{stop},{i + 1},0,0")
    files["stop_times.txt"] = "\n".join(st) + "\n"
    stops = ["stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station"]
    for i in range(N_STOPS):
        lat, lon = latlon(i * STOP_EVERY_M, 0.0)
        stops.append(f"S{i},Stop {i},{lat:.7f},{lon:.7f},0,")
    stops += ["FQ0,Quay A,59.10,5.60,0,", "FQ1,Quay B,59.12,5.62,0,"]
    files["stops.txt"] = "\n".join(stops) + "\n"
    sh = ["shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence"]
    for k, (x, y) in enumerate(shape_pts):
        lat, lon = latlon(x, y)
        sh.append(f"SH1,{lat:.7f},{lon:.7f},{k + 1}")
    files["shapes.txt"] = "\n".join(sh) + "\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in files.items():
            z.writestr(name, text)
    return buf.getvalue()


def write_raw(root, seed=1):
    """Writes the raw day into <root>/buspulse-raw. Returns facts the tests check against."""
    left, pings = line_day(seed, t0=T0)
    dep = left.attrs["dep"]
    trips = journeys(dep)
    end = max(max(a) for _, a in trips.values())

    # estimated timetable: every 2 min on DAY, stopping before T2 has finished
    t2_end = max(trips["T2"][1])
    last_day_snap = t2_end - 200
    snaps = list(np.arange(T0 - 300, last_day_snap, 120.0))
    for k, s in enumerate(snaps):
        drop = ("T1", 0) if k == len(snaps) - 1 else None        # newest snapshot lacks T1's first actual
        put(root, "buspulse-raw", f"estimated_timetable/date={DAY}/estimated_timetable_{stamp(s)}.xml.gz",
            gzip.compress(et_snapshot(s, trips, drop)))
    # next UTC day: the rest of T2's actual times
    s_next = datetime(2026, 10, 31, 0, 10, tzinfo=timezone.utc).timestamp()
    put(root, "buspulse-raw", f"estimated_timetable/date={NEXT}/estimated_timetable_{stamp(s_next)}.xml.gz",
        gzip.compress(et_snapshot(s_next, {k: v for k, v in trips.items() if k == "T2"})))

    # vehicle positions: a snapshot every 30 s holding each bus's newest ping
    pings = pings.sort_values("t")
    n_vp = 0
    for s in np.arange(T0 - 60, end + 60, 30.0):
        vehicles = []
        for veh, d in pings[pings["t"] <= s].groupby("vehicle_id"):
            p = d.iloc[-1]
            lat, lon = latlon(p["x"], p["y"])
            vehicles.append({"vehicleId": veh, "lastUpdated": iso_z(p["t"]),
                             "line": {"lineRef": "L1", "lineName": "1", "publicCode": "1"},
                             "location": {"latitude": lat, "longitude": lon},
                             "bearing": 90.0, "speed": None, "delay": 0.0, "occupancyStatus": "noData"})
        n_vp += len(vehicles)
        put(root, "buspulse-raw", f"vehicle_positions/date={DAY}/vehicle_positions_{stamp(s)}.json.gz",
            gzip.compress(json.dumps({"data": {"vehicles": vehicles}}).encode()))

    # one service alert, and the timetable
    sx = (f'<?xml version="1.0"?><Siri xmlns="{NS}"><ServiceDelivery><SituationExchangeDelivery><Situations>'
          f"<PtSituationElement><CreationTime>{iso_local(T0)}</CreationTime>"
          f"<SituationNumber>KOL:SituationNumber:1</SituationNumber><VersionedAtTime>{iso_local(T0)}</VersionedAtTime>"
          f"<Progress>open</Progress><ValidityPeriod><StartTime>{iso_local(T0)}</StartTime>"
          f"<EndTime>{iso_local(T0 + 3600)}</EndTime></ValidityPeriod><Severity>normal</Severity>"
          f"<ReportType>incident</ReportType><Planned>false</Planned>"
          f'<Summary xml:lang="NO">Forsinket på grunn av mye trafikk</Summary><Affects><Networks><AffectedNetwork>'
          f"<AffectedLine><LineRef>L1</LineRef></AffectedLine></AffectedNetwork></Networks></Affects>"
          f"</PtSituationElement></Situations></SituationExchangeDelivery></ServiceDelivery></Siri>")
    put(root, "buspulse-raw", f"service_alerts/date={DAY}/service_alerts_{stamp(T0)}.xml.gz", gzip.compress(sx.encode()))
    shape_pts = [(float(k * 50), 0.0) for k in range(int((N_STOPS - 1) * STOP_EVERY_M / 50) + 1)]
    put(root, "buspulse-raw", f"static_timetable/date={DAY}/static_timetable_{stamp(T0 - 7200)}.zip",
        gtfs_zip(trips, shape_pts))
    return {"n_vp_rows": n_vp, "n_et_files_day": len(snaps), "trips": trips}
