# Local Silver Parquet -> Gold Parquet tables + DuckDB database. No Spark or Swift.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app/gold
COPY gold/requirements.txt /app/gold/requirements.txt
RUN pip install --no-cache-dir -r /app/gold/requirements.txt
COPY gold/build_gold.py gold/run_gold.py /app/gold/
ENV PYTHONUNBUFFERED=1 TZ=UTC GOLD_WORK_DIR=/work/gold
RUN useradd -u 1000 -m gold && mkdir -p /work /data/tables && chown -R gold:gold /work /data
USER gold
ENTRYPOINT ["tini", "--", "python", "/app/gold/run_gold.py"]
