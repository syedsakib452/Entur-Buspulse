# Raw Swift objects -> local Parquet in tables/bronze-layer. No Spark or Silver code.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*

COPY bronze-layer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY bronze-layer/build_bronze_tables.py /app/build_bronze_tables.py
COPY bronze-layer/run_bronze_range.py /app/run_bronze_range.py
ENV PYTHONUNBUFFERED=1 TZ=UTC OUT_DIR=/data/tables/bronze-layer
RUN useradd -u 1000 -m bronze && mkdir -p /work /data/tables && chown -R bronze:bronze /work /data
USER bronze
ENTRYPOINT ["tini", "--", "python", "/app/build_bronze_tables.py"]
