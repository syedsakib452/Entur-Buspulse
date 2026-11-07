# Image for the bronze + silver batch job (separate from the collectors' image).
# Built by .github/workflows/build-tables-image.yml, run by k8s-tables/cronjob.yaml.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app/tables
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY build_bronze_tables.py build_silver_tables.py run_days.py ./

# Runs as a non-root user (set in the CronJob); temporary files go to TMPDIR.
ENTRYPOINT ["python", "-u", "run_days.py"]
