FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates tini \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY analysis/silver-layer/spark_jobs/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") \
 && curl -fsSL -o "$JARS/hadoop-aws-3.3.4.jar" \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o "$JARS/aws-java-sdk-bundle-1.12.262.jar" \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /app
RUN mkdir -p /app/spark_jobs
COPY analysis/bronze-layer/spark_jobs/ /app/spark_jobs/
COPY analysis/silver-layer/spark_jobs/ /app/spark_jobs/
ENV PYTHONPATH=/app PYSPARK_PYTHON=python PYTHONUNBUFFERED=1 SPARK_LOCAL_DIR=/work/spark TZ=UTC

RUN useradd -u 1000 -m spark && mkdir -p /work && chown spark /work
USER 1000
ENTRYPOINT ["tini", "--", "python", "-m", "spark_jobs.run_pipeline"]
