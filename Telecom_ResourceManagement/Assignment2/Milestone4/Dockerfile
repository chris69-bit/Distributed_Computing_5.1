# TeleRM S3 image: one image, every service (registry, orchestrator, telemetry, gateways,
# sites, load generator). Which one runs is chosen by the container's command.
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN python -c "import psutil" 2>/dev/null || pip install --no-cache-dir "psutil>=5.9"
COPY telerm/ telerm/
COPY config.json config_m2.json config_m3.json run_demo.py perf_experiment.py m3_run.py m4_run.py ./
