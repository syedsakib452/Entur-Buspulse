# Archived implementations

These files are retained for historical reference and are not imported, copied into Docker images, or used by the active Bronze → Silver → Gold flow.

- `legacy-swift-spark-pipeline/`: the former combined Spark pipeline that staged and published intermediate Bronze/Silver data in separate Swift containers, plus its Kubernetes manifests, Spark Bronze code and tests.
- `legacy-silver-operations/`: the former long-running Silver loop, tmux helper, entrypoint and Silver-only Compose file.
- `legacy-docker-runner/`: the generic multi-day Docker runner that duplicated layer-specific execution.

`legacy-swift-spark-pipeline/previous-versions/` also retains the replaced Swift-era shared helper, Gold Swift publisher, Dockerfiles and dependency pins. These are archival snapshots only.
