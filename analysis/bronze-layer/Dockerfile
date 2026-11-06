# BRONZE layer image: raw files in Swift -> bronze Parquet tables in Swift. Nothing else.
# Build from the repo root:  docker build -f analysis/bronze-layer/Dockerfile -t buspulse-bronze .
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY analysis/bronze-layer/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
# shared helpers + the day-by-day runner, then only the bronze code
COPY analysis/silver-layer/spark_jobs/__init__.py \
     analysis/silver-layer/spark_jobs/common.py \
     analysis/silver-layer/spark_jobs/run_pipeline.py /app/spark_jobs/
COPY analysis/bronze-layer/spark_jobs/bronze.py \
     analysis/bronze-layer/spark_jobs/parsers.py /app/spark_jobs/

ENV PYTHONPATH=/app PYSPARK_PYTHON=python PYTHONUNBUFFERED=1 TZ=UTC \
    SPARK_LOCAL_DIR=/work/spark SWIFT_CACHE_DIR=/work/swift
RUN useradd -u 1000 -m spark && mkdir -p /work && chown spark /work
USER 1000
# the stage is fixed in the image: this container can only build bronze
ENTRYPOINT ["tini", "--", "python", "-m", "spark_jobs.run_pipeline", "--stages", "bronze"]
