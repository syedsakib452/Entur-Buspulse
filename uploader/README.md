# Uploader

Moves finished bronze data from the VM to Swift, once a night.

For every `data/<feed>/date=YYYY-MM-DD/` folder **older than today (UTC)**:

1. **Compress** every file that isn't compressed yet (`.xml` → `.xml.gz`, `.json` → `.json.gz`).
   Lossless: each file is unzipped and compared with the original before the original is removed.
2. **Upload** to the `buspulse-raw` container with the same path as its name, e.g.
   `estimated_timetable/date=2026-10-21/estimated_timetable_20261021T120000Z.xml.gz`.
   Swift checks each file's MD5 on arrival, so a successful upload is a byte-for-byte copy.
   Files already in Swift are skipped, so re-running is always safe. Each file gets 3 attempts.
3. **Delete** the local file, only after its upload was confirmed. Empty day folders are removed.

Today's folders are never touched, because the collectors are still writing to them.
Log: `logs/uploader.log` (`started`, `finished: …` with counts, and one `ERROR` line per failed file).
The program exits with code 1 if any file failed; those files stay on the VM and are retried next night.

## Setup on the VM (once)

1. Add the `uploader` service from `compose-uploader-snippet.yaml` to `compose.yaml`.
2. Make sure `~/openstack.env` exists (the `OS_...=value` lines, no `export`, no quotes).
3. Build and try it without changing anything:

   ```bash
   cd ~/buspulse
   docker compose build uploader
   docker compose run --rm -e DRY_RUN=true uploader
   ```

4. Real run (by hand the first time):

   ```bash
   docker compose run --rm uploader
   ```

5. Every night at 02:00 UTC:

   ```bash
   (crontab -l 2>/dev/null; echo "0 2 * * * cd $HOME/buspulse && docker compose run --rm uploader >> $HOME/uploader-cron.log 2>&1") | crontab -
   ```

## Settings (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `SWIFT_CONTAINER` | `buspulse-raw` | Target container (created if missing) |
| `DELETE_LOCAL` | `true` | `false` = upload but keep local files |
| `DRY_RUN` | `false` | `true` = only log what would be done |
| `WORKERS` | `4` | Parallel uploads (lower it if the storage proxy returns 400 errors) |
| `RETRIES` | `3` | Attempts per file |
| `DATA_DIR` / `LOG_DIR` | `/data` / `/logs` | Folders inside the container |
| `OS_*` | | OpenStack login, from `openstack.env` |

## Later: Kubernetes

The same image (`ghcr.io/<owner>/entur-buspulse-uploader`, built by `build-uploader.yml`) runs as a
CronJob with the same environment variables (`OS_*` from a Secret) and the collectors' data volume mounted at `/data`.

## Tests

```bash
cd uploader && go test ./...
```

Runs the uploader against an in-memory Swift server: checks compression is lossless, uploads match,
today's folder is untouched, and a second run finds nothing left to do.
