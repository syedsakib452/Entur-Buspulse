# Bronze + silver batch job on Kubernetes

Separate from the collectors (`k8s/`): own namespace `buspulse-tables`, own image
(`ghcr.io/syedsakib452/entur-buspulse-tables`, built from `tables/` by `build-tables-image.yml`).

A CronJob runs `tables/run_days.py` every night at 04:00 UTC. It builds bronze
(`buspulse-raw` -> `buspulse-bronze`) and silver (`buspulse-bronze` -> `buspulse-silver`, buses only)
for every finished day since `START_DATE` that is not in `buspulse-silver` yet. Normally that is just
yesterday; the first run backfills all earlier days. A failed day is retried the next night.

| File | What it creates |
|---|---|
| `namespace.yaml` | Namespace `buspulse-tables` |
| `configmap.yaml` | `START_DATE`, `END_DATE` (empty = up to yesterday), download workers, temp folder |
| `cronjob.yaml` | CronJob `bronze-silver`: 1 Gi memory requested, 2 Gi limit, 5 Gi temp space, max 48 h per run |
| `kustomization.yaml` | Applies the above; pins the image tag |

## Setup (once)

```bash
kubectl apply -f k8s-tables/namespace.yaml
kubectl create secret generic openstack-credentials -n buspulse-tables --from-env-file=$HOME/k8s-openstack.env
kubectl apply -k k8s-tables/
```

## Run now, follow, check

```bash
kubectl create job -n buspulse-tables --from=cronjob/bronze-silver bronze-silver-manual
kubectl logs -n buspulse-tables -f job/bronze-silver-manual
kubectl get jobs -n buspulse-tables
swift list buspulse-silver | cut -d/ -f2 | sort | uniq -c
```

## New version

Push a change under `tables/`, wait for `build-tables-image` to be green, set `newTag` in
`kustomization.yaml` to its `sha-<commit>` tag, then `kubectl apply -k k8s-tables/`.

## Remove

```bash
kubectl delete -k k8s-tables/     # namespace, CronJob and Secret; data in Swift is kept
```
