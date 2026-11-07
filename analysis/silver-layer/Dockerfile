# Local Bronze Parquet -> cleaned/deduplicated Silver Parquet. Spark runs in local mode.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app
COPY silver-layer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

RUN mkdir -p /app/spark_jobs
COPY silver-layer/spark_jobs/__init__.py \
     silver-layer/spark_jobs/common.py \
     silver-layer/spark_jobs/run_silver_tables.py \
     silver-layer/spark_jobs/silver.py \
     silver-layer/spark_jobs/stop_analysis.py \
     silver-layer/spark_jobs/stop_geometry.py /app/spark_jobs/

ENV PYTHONPATH=/app PYSPARK_PYTHON=python PYTHONUNBUFFERED=1 TZ=UTC \
    TABLES_DIR=/data/tables SILVER_LOCAL_DIR=/data/tables/silver-layer \
    DATA_ROOT=/work/runtime SPARK_LOCAL_DIR=/work/spark \
    SPARK_MASTER=local[2] SPARK_DRIVER_MEMORY=1400m SPARK_SHUFFLE_PARTITIONS=16
RUN useradd -u 1000 -m spark && mkdir -p /work /data/tables && chown -R spark:spark /work /data
USER spark
ENTRYPOINT ["tini", "--", "python", "-m", "spark_jobs.run_silver_tables"]
