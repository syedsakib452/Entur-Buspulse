# Dashboard image. Build from the repository root (the build needs dashboard/):
#   docker build -f dashboard/Dockerfile -t buspulse-dashboard .
# Gold is NOT copied into the image (it is rebuilt every night, and .dockerignore excludes tables/).
# Mount the gold folder at /data/gold when starting the container:
#   docker run -d --name buspulse-dashboard -p 8501:8501 \
#     -v "$PWD/tables/gold-layer:/data/gold:ro" buspulse-dashboard
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    BUSPULSE_GOLD_DIR=/data/gold \
    BUSPULSE_MAP_DIR=/app/dashboard/map

WORKDIR /app
COPY dashboard/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# app.py, build_map.py and the static road map (dashboard/map/*.parquet)
COPY dashboard/ /app/dashboard/

RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health', timeout=4)"

CMD ["streamlit", "run", "dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
