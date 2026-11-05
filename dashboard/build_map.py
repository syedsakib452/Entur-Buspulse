#!/usr/bin/env python3
"""
build_map.py - static road map of the bus network (Stavanger / Rogaland), built from GTFS.

    python dashboard/build_map.py --gtfs <folder with gtfs_*.parquet> --out dashboard/map

Why GTFS and not a map service: `gtfs_shapes` holds the exact road geometry every bus trip follows
(a point every ~10 m, with the distance driven so far). That is the road network we have data for,
it needs no internet, no API key and no tile server, and it lines up exactly with our stop IDs.

How a road segment is made: for every pair of consecutive stops (from_stop -> to_stop) we take the
shape used by most trips between them and cut it at the two stops' `shape_dist_traveled` values.
The result is one polyline per stop pair, i.e. one coloured piece of road per gold segment.

Output (small static files, built once per GTFS version):
  road_segments.parquet   from_stop_ref, to_stop_ref, length_m, n_trips, routes, path [[lon, lat], ...]
  stops.parquet           stop_ref, stop_name, lat, lon (stops served by bus)
  bus_roads.geojson       the same segments as GeoJSON LineStrings, for QGIS or any other GIS tool
Ferries are left out (their shapes cross the sea, not roads).
"""
import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from shapely.geometry import LineString

BUS_ROUTE_TYPES = {"3"} | {str(x) for x in range(700, 800)}   # same rule as the silver layer
SIMPLIFY_DEG = 0.00003    # ~2-3 m: removes points on straight road, keeps every bend


def read(folder, name, cols=None):
    return pd.read_parquet(Path(folder) / f"gtfs_{name}.parquet", columns=cols)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gtfs", required=True, help="folder with gtfs_*.parquet (bronze GTFS tables)")
    ap.add_argument("--out", default="dashboard/map")
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    routes = read(a.gtfs, "routes", ["route_id", "route_short_name", "route_type"])
    bus_routes = routes[routes["route_type"].astype(str).isin(BUS_ROUTE_TYPES)]
    trips = read(a.gtfs, "trips", ["trip_id", "route_id", "shape_id"])
    trips = trips[trips["route_id"].isin(bus_routes["route_id"]) & trips["shape_id"].astype(str).str.strip().ne("")]
    st = read(a.gtfs, "stop_times", ["trip_id", "stop_id", "stop_sequence", "shape_dist_traveled"])
    st = st[st["trip_id"].isin(trips["trip_id"])].copy()
    st["seq"] = pd.to_numeric(st["stop_sequence"], errors="coerce")
    st["dist"] = pd.to_numeric(st["shape_dist_traveled"], errors="coerce")
    st = st.dropna(subset=["seq", "dist"]).sort_values(["trip_id", "seq"])
    st = st.merge(trips, on="trip_id")
    g = st.groupby("trip_id", sort=False)
    st["to_stop_ref"] = g["stop_id"].shift(-1)
    st["to_dist"] = g["dist"].shift(-1)
    pairs = st.dropna(subset=["to_stop_ref"]).rename(columns={"stop_id": "from_stop_ref", "dist": "from_dist"})
    print(f"bus trips: {trips['trip_id'].nunique():,} | stop-to-stop legs: {len(pairs):,}")

    # per stop pair: the shape most trips use, its cut distances, how many trips, which routes
    usage = (pairs.groupby(["from_stop_ref", "to_stop_ref", "shape_id", "from_dist", "to_dist"])
                  .size().rename("n").reset_index()
                  .sort_values("n", ascending=False, kind="stable")
                  .drop_duplicates(["from_stop_ref", "to_stop_ref"]))
    names = pairs.merge(bus_routes[["route_id", "route_short_name"]], on="route_id")
    info = names.groupby(["from_stop_ref", "to_stop_ref"]).agg(
        n_trips=("trip_id", "size"),
        routes=("route_short_name", lambda s: sorted(set(s), key=lambda x: (len(x), x))))
    segs = usage.merge(info.reset_index(), on=["from_stop_ref", "to_stop_ref"])
    print(f"distinct stop pairs (road segments): {len(segs):,}")

    shapes = read(a.gtfs, "shapes", ["shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon", "shape_dist_traveled"])
    shapes = shapes[shapes["shape_id"].isin(segs["shape_id"])]
    for c in ["shape_pt_sequence", "shape_pt_lat", "shape_pt_lon", "shape_dist_traveled"]:
        shapes[c] = pd.to_numeric(shapes[c], errors="coerce")
    shapes = shapes.dropna().sort_values(["shape_id", "shape_pt_sequence"])
    by_shape = {sid: (d["shape_dist_traveled"].to_numpy(), d["shape_pt_lon"].to_numpy(), d["shape_pt_lat"].to_numpy())
                for sid, d in shapes.groupby("shape_id", sort=False)}

    paths, lengths, bad = [], [], 0
    for r in segs.itertuples(index=False):
        dist, lon, lat = by_shape[r.shape_id]
        d0, d1 = r.from_dist, r.to_dist
        if not (d1 > d0) or dist.size < 2:
            paths.append(None); lengths.append(np.nan); bad += 1
            continue
        inside = (dist > d0) & (dist < d1)
        xs = np.concatenate([[np.interp(d0, dist, lon)], lon[inside], [np.interp(d1, dist, lon)]])
        ys = np.concatenate([[np.interp(d0, dist, lat)], lat[inside], [np.interp(d1, dist, lat)]])
        line = LineString(np.column_stack([xs, ys])).simplify(SIMPLIFY_DEG, preserve_topology=False)
        paths.append([[round(x, 6), round(y, 6)] for x, y in line.coords])
        lengths.append(d1 - d0)
    segs["path"] = paths
    segs["length_m"] = lengths
    segs = segs.dropna(subset=["path"])
    print(f"segments without a usable cut (to_dist <= from_dist): {bad:,} skipped")

    # cross-check: GTFS distance vs length of the drawn line (haversine), should agree closely
    def line_m(p):
        p = np.radians(np.asarray(p))
        dlon, dlat = np.diff(p[:, 0]), np.diff(p[:, 1])
        h = np.sin(dlat / 2) ** 2 + np.cos(p[:-1, 1]) * np.cos(p[1:, 1]) * np.sin(dlon / 2) ** 2
        return float((2 * 6371000 * np.arcsin(np.sqrt(h))).sum())
    ratio = segs["path"].map(line_m) / segs["length_m"]
    print(f"drawn length / GTFS length: median {ratio.median():.3f}, 5-95% {ratio.quantile(.05):.3f}-{ratio.quantile(.95):.3f}")

    keep = ["from_stop_ref", "to_stop_ref", "length_m", "n_trips", "routes", "path"]
    segs[keep].to_parquet(out / "road_segments.parquet", index=False, compression="zstd")

    stops = read(a.gtfs, "stops", ["stop_id", "stop_name", "stop_lat", "stop_lon"])
    used = set(segs["from_stop_ref"]) | set(segs["to_stop_ref"])
    stops = stops[stops["stop_id"].isin(used)].rename(columns={"stop_id": "stop_ref", "stop_lat": "lat", "stop_lon": "lon"})
    stops["lat"] = pd.to_numeric(stops["lat"], errors="coerce")
    stops["lon"] = pd.to_numeric(stops["lon"], errors="coerce")
    stops.to_parquet(out / "stops.parquet", index=False, compression="zstd")

    features = [{"type": "Feature",
                 "properties": {"from_stop_ref": r.from_stop_ref, "to_stop_ref": r.to_stop_ref,
                                "length_m": round(float(r.length_m), 1), "n_trips": int(r.n_trips), "routes": ",".join(r.routes)},
                 "geometry": {"type": "LineString", "coordinates": r.path}} for r in segs[keep].itertuples(index=False)]
    (out / "bus_roads.geojson").write_text(json.dumps({"type": "FeatureCollection", "features": features}, separators=(",", ":")))

    pts = int(segs["path"].map(len).sum())
    print(f"written to {out.resolve()}: {len(segs):,} road segments ({pts:,} points), {len(stops):,} stops")
    for f in ["road_segments.parquet", "stops.parquet", "bus_roads.geojson"]:
        print(f"  {f}: {(out / f).stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
