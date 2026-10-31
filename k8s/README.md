# Collectors on Kubernetes

The four collectors run in the k3s cluster (see `infra/`) in the namespace `buspulse`.
They upload straight to Swift (`STORAGE_MODE=swift`). If an upload fails, the file waits in
a fallback folder (`/buffer`, an `emptyDir` volume) and is retried every 30 s.
While the buffer has files, new data goes straight to the buffer; once it is empty, direct uploads resume.

| File | What it creates |
|---|---|
| `namespace.yaml` | Namespace `buspulse` |
| `configmap.yaml` | Settings: storage mode, Swift container, buffer, logging |
| `secret.example.yaml` | Template only. The real Secret is created from a file (below) |
| `vehicle-positions.yaml`, `estimated-timetable.yaml`, `service-alerts.yaml` | One Deployment each: 1 pod, liveness probe, CPU/memory limits, non-root |
| `static-timetable.yaml` | CronJob, once a day at 03:00 UTC, retried on failure |
| `kustomization.yaml` | Applies all of the above; sets the image tag in one place |

All commands run on a machine with `kubectl` access (we use `budpulse1`).

## 1. One-time setup

**OpenStack login for the cluster** (an application credential, not your password):

```bash
set -a; source ~/openstack.env; set +a
openstack application credential create buspulse-k8s \
  -f value -c id -c secret
```

Put the two printed values into a file **outside the repo**, e.g. `~/k8s-openstack.env`:

```
OS_AUTH_TYPE=v3applicationcredential
OS_AUTH_URL=http://cloud.cs.ux.uis.no:5000/v3/
OS_IDENTITY_API_VERSION=3
OS_REGION_NAME=RegionOne
OS_INTERFACE=public
OS_APPLICATION_CREDENTIAL_ID=<id>
OS_APPLICATION_CREDENTIAL_SECRET=<secret>
```

**Test container in Swift:**

```bash
swift post buspulse-raw-test
```

**Namespace and Secrets:**

```bash
kubectl apply -f k8s/namespace.yaml
kubectl create secret generic openstack-credentials -n buspulse --from-env-file=$HOME/k8s-openstack.env
# Only if the ghcr.io image is private (token with read:packages):
kubectl create secret docker-registry ghcr-pull -n buspulse \
  --docker-server=ghcr.io --docker-username=<github-user> --docker-password=<token>
```

## 2. Deploy

```bash
kubectl apply -k k8s/
```

## 3. Verify

```bash
kubectl get pods -n buspulse -o wide                 # 3 pods Running, spread over the workers
kubectl logs -n buspulse deploy/vehicle-positions    # "started", no errors
swift list buspulse-raw-test | cut -d/ -f1,2 | sort | uniq -c   # files arriving per feed/day
kubectl create job -n buspulse --from=cronjob/static-timetable static-test   # run the daily job now
kubectl get jobs -n buspulse                         # static-test: Complete
```

**Fallback test:** point one collector at a Swift container that does not exist yet, so every
upload fails. Files pile up in the buffer. Then create the container: within a minute the
retry thread uploads them and the buffer empties, without restarting anything.

```bash
kubectl set env -n buspulse deploy/vehicle-positions SWIFT_CONTAINER=buspulse-buffer-test
sleep 180
kubectl exec -n buspulse deploy/vehicle-positions -- find /buffer -type f | wc -l   # > 0
swift post buspulse-buffer-test                                                      # "Swift is back"
sleep 60
kubectl exec -n buspulse deploy/vehicle-positions -- find /buffer -type f | wc -l   # 0
swift list buspulse-buffer-test | wc -l                                              # the buffered files
kubectl logs -n buspulse deploy/vehicle-positions | grep buffer                      # "saved to buffer" ... "uploaded N file(s)"
# clean up: back to the ConfigMap setting, delete the test container
kubectl set env -n buspulse deploy/vehicle-positions SWIFT_CONTAINER-
swift delete buspulse-buffer-test
```

**Recovery tests (kill-a-pod / worker loss):**

```bash
kubectl delete pod -n buspulse -l app=vehicle-positions   # a new pod starts within seconds
kubectl drain k3s-worker1 --ignore-daemonsets --delete-emptydir-data   # pods move to worker2
kubectl uncordon k3s-worker1
```

## 4. Switch over to the real container

```bash
kubectl patch configmap collector-settings -n buspulse --type merge -p '{"data":{"SWIFT_CONTAINER":"buspulse-raw"}}'
kubectl rollout restart deploy -n buspulse
```

Then on the VM: `docker compose down` for the collectors, run the uploader once
(`docker compose run --rm uploader`), and remove its cron line.

## Remove

```bash
kubectl delete -k k8s/      # removes the namespace and everything in it, including the Secrets
```

Data in Swift is not touched.
