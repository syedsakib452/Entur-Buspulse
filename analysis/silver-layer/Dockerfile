# SILVER layer image: finished bronze days -> silver tables (cleaning + GPS stop analysis). Nothing else.
# Build from the repo root:  docker build -f analysis/silver-layer/Dockerfile -t buspulse-silver .
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY analysis/silver-layer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
# shared helpers + the day-by-day runner, then only the silver code (no bronze, no tests)
COPY analysis/silver-layer/spark_jobs/__init__.py \
     analysis/silver-layer/spark_jobs/common.py \
     analysis/silver-layer/spark_jobs/run_pipeline.py \
     analysis/silver-layer/spark_jobs/silver.py \
     analysis/silver-layer/spark_jobs/stop_analysis.py \
     analysis/silver-layer/spark_jobs/stop_geometry.py /app/spark_jobs/
COPY analysis/silver-layer/run_silver_loop.sh /app/run_silver_loop.sh

ENV PYTHONPATH=/app PYSPARK_PYTHON=python PYTHONUNBUFFERED=1 TZ=UTC \
    SPARK_LOCAL_DIR=/work/spark SWIFT_CACHE_DIR=/work/swift
RUN useradd -u 1000 -m spark && mkdir -p /work && chown spark /work
USER 1000
# The stages are fixed in the image: this container only builds silver, from bronze days marked done.
# Default: one pass (Kubernetes CronJob). Docker Compose uses run_silver_loop.sh instead (a pass every hour).
ENTRYPOINT ["tini", "--", "python", "-m", "spark_jobs.run_pipeline", "--stages", "clean", "stop_analysis"]
