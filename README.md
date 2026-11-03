# Entur-Buspulse

Collects live bus data for Kolumbus (Entur open APIs, codespace `KOL`), stores it in a
medallion architecture (bronze → silver → gold) on UiS OpenStack, and serves the results.

## Bronze collectors

Four collectors, each with its own `ET-Client-Name`. They save Entur's raw responses, untouched,
gzipped, to the Swift container `buspulse-raw`:

| Collector | Feed | Every | Saves |
|---|---|---|---|
| `vehicle_positions` | Vehicles GraphQL – live bus positions | 30 s | every response (`.json.gz`) |
| `estimated_timetable` | SIRI ET – arrivals/departures per stop | 60 s | every response (`.xml.gz`) |
| `service_alerts` | SIRI SX – disruptions | 60 s | only when alerts change (`.xml.gz`) |
| `static_timetable` | GTFS – planned timetable | daily, 03:00 UTC | the zip (`.zip`) |

Object names (UTC): `<feed>/date=YYYY-MM-DD/<feed>_YYYYMMDDTHHMMSSZ.<ext>`

If Swift fails, data waits in a local buffer and is uploaded later, so nothing is lost
(see `k8s/README.md`).

```
collectors/   one script per feed (client name + interval at the top of each file)
common/       settings, Entur client, storage (Swift + buffer), metrics, health check
tests/        pytest: storage, buffer, retries, timing, metrics
```

## Running it

The collectors run on a k3s cluster on UiS OpenStack. Set it up in this order:

1. `infra/`: create the VMs and install k3s
2. `k8s/`: deploy the collectors
3. `monitoring/`: Prometheus, Grafana, Loki, alerts to Discord

The image is built and tested by GitHub Actions (`build-image.yml`) and published as
`ghcr.io/syedsakib452/entur-buspulse`.

**Locally** (saves to `./data` instead of Swift):

```bash
docker compose up -d --build                     # all collectors
# or without Docker:
pip install -r requirements.txt
DATA_DIR=./data LOG_DIR=./logs python -m collectors.vehicle_positions
```

**Tests:**

```bash
pip install -r requirements.txt pytest && pytest
```

`uploader/` (Go) moved data from the old VM setup to Swift. It is not needed on Kubernetes,
where the collectors upload directly.
