# --- Build stage: install deps with compiled extensions ---
FROM python:3.11-slim AS builder
RUN apt-get update && apt-get install -y --no-install-recommends gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

# --- Runtime stage ---
FROM python:3.11-slim
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
RUN apt-get update && \
    apt-get install -y --no-install-recommends librdkafka1 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /install /usr/local
WORKDIR /app
COPY netbox_device_state.avsc .
COPY bridge.py .
RUN groupadd -r bridge && useradd -r -g bridge bridge
USER bridge
EXPOSE 8090
ENTRYPOINT ["gunicorn", "-b", "0.0.0.0:8090", "-w", "2", "--timeout", "120", "bridge:app"]
