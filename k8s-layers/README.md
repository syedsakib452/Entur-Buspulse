# One job per layer: bronze, silver, gold

Three images and three CronJobs instead of one. Each job does only its own layer, in its own pod,
with its own memory, and they never write to the same place:

| job | image (Dockerfile) | reads | writes | runs (UTC) |
|---|---|---|---|---|
| `bronze-layer` | `analysis/bronze-layer/Dockerfile` | `buspulse-raw` (complete days only) | `buspulse-bronze`, marker `buspulse-silver/_done/bronze/<day>` | 04:10 |
| `silver-layer` | `analysis/silver-layer/Dockerfile` | bronze days with a done marker | `buspulse-silver` tables, markers `_done/clean`, `_done/stop_analysis` | 16:10 (12 h after bronze) |
| `gold-layer` | `analysis/gold/Dockerfile` | silver days with both markers | `buspulse-gold/builds/<id>/` + `latest.json` | 19:40 |

How they stay out of each other's way
- A layer only reads days the previous layer has **finished** (done markers, written last).
  If the previous layer is still running, or failed, the day is simply picked up on the next run.
- Each job has `concurrencyPolicy: Forbid`, so a slow run is never doubled.
- Gold uploads a whole new build first and only then switches `latest.json` to it, so readers
  (the dashboard) never see a half-written gold layer. The last 3 builds are kept.
- A failed gold build uploads nothing; the previous build stays live.

The schedules are only a sensible order: correctness comes from the markers, not the times.
Silver also waits `SILVER_DELAY_HOURS` (12) after the bronze it needs was finished.

Use either this `silver-layer` CronJob or the Docker container on a VM
(`analysis/silver-layer/run_silver_tmux.sh`), not both: they would build the same days twice.
If you use the container, suspend the CronJob:
`kubectl patch cronjob silver-layer -n buspulse-tables -p '{"spec":{"suspend":true}}'`

## Deploy
1. Push; the `build-layer-images` workflow tests the code and builds the three images.
   Make the three new packages public on GitHub (or add an image pull secret).
2. The Secret `swift-credentials` (OS_* variables) must exist in `buspulse-tables`
   (same as for the combined spark-pipeline job; see `analysis/spark_pipeline/k8s-spark/secret.example.yaml`).
3. Set `OS_AUTH_URL` in `configmap.yaml`, then: `kubectl apply -k k8s-layers`
4. Suspend the combined job so the layers are not built twice:
   `kubectl patch cronjob spark-pipeline -n buspulse-tables -p '{"spec":{"suspend":true}}'`
5. Test each layer once, in order:
   `kubectl create job bronze-test --from=cronjob/bronze-layer -n buspulse-tables`, then silver, then gold.

## Reading gold
`buspulse-gold/latest.json` names the live build; its files are under `prefix` (e.g. `builds/20261107T092000Z/`).
