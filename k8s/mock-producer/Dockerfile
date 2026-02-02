# Mock gNMI telemetry producer
# Builds for linux/arm64 (RPi4 k3s cluster)
#
# Build:
#   docker buildx build --platform linux/arm64 -t mock-telemetry-producer:latest .
#
# Run standalone (for testing):
#   docker run --rm -e KAFKA_BOOTSTRAP=host.docker.internal:9092 \
#     -e SCHEMA_REGISTRY_URL=http://host.docker.internal:8081 \
#     mock-telemetry-producer:latest

FROM python:3.11-slim AS builder

# Avoid interactive prompts and set pip to not cache
ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build deps for confluent-kafka (librdkafka)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

# ---------------------------------------------------------------------------
# Runtime stage — minimal image
# ---------------------------------------------------------------------------
FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Runtime librdkafka dependency
RUN apt-get update && \
    apt-get install -y --no-install-recommends librdkafka1 && \
    rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from builder
COPY --from=builder /install /usr/local

WORKDIR /app
COPY interface_telemetry.avsc .
COPY producer.py .

# Non-root
RUN groupadd -r telemetry && useradd -r -g telemetry telemetry
USER telemetry

ENTRYPOINT ["python", "-u", "producer.py"]
