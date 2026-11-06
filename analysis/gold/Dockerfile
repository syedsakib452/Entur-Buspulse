# GOLD layer image: finished silver days -> gold tables + gold.duckdb (DuckDB, no Spark/Java).
# Build from the repo root:  docker build -f analysis/gold/Dockerfile -t buspulse-gold .
FROM python:3.12-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends tini ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY analysis/gold/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app/gold
COPY analysis/gold/build_gold.py analysis/gold/run_gold.py /app/gold/

ENV PYTHONUNBUFFERED=1 TZ=UTC GOLD_WORK_DIR=/work/gold
RUN useradd -u 1000 -m gold && mkdir -p /work && chown gold /work
USER 1000
# time-zone support for DuckDB, fetched now so the job needs no download at run time
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL icu'); c.execute('LOAD icu')" || true
ENTRYPOINT ["tini", "--", "python", "/app/gold/run_gold.py"]
