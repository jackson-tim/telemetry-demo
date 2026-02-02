#!/usr/bin/env python3
"""
NetBox Enrichment Consumer (v2 — CDC-backed)
=============================================
Reads raw interface telemetry from Kafka, enriches each event with device/interface
metadata from a compacted netbox-changes topic, and publishes to an enriched topic.

NO direct NetBox API calls at query time. The cache is populated entirely
from the netbox-changes Kafka topic (CDC from the netbox-bridge service).

On startup:
  1. Read netbox-changes from offset 0 → build local cache
  2. Subscribe to interface-telemetry-raw
  3. In background, keep consuming netbox-changes for live updates

Env vars:
    KAFKA_BOOTSTRAP       - Kafka broker(s)  (default: kafka:9092)
    SCHEMA_REGISTRY_URL   - Schema Registry   (default: http://schema-registry:8081)
    CONSUMER_GROUP        - Consumer group id  (default: telemetry-enrichment)
    LOG_LEVEL             - Logging level      (default: INFO)
    RAW_TOPIC             - Source topic        (default: interface-telemetry-raw)
    ENRICHED_TOPIC        - Destination topic   (default: interface-telemetry-enriched)
    NETBOX_TOPIC          - CDC topic           (default: netbox-changes)
    POLL_TIMEOUT          - Consumer poll timeout in seconds  (default: 1.0)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
import threading
from typing import Any

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "telemetry-enrichment")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
RAW_TOPIC = os.environ.get("RAW_TOPIC", "interface-telemetry-raw")
ENRICHED_TOPIC = os.environ.get("ENRICHED_TOPIC", "interface-telemetry-enriched")
NETBOX_TOPIC = os.environ.get("NETBOX_TOPIC", "netbox-changes")
POLL_TIMEOUT = float(os.environ.get("POLL_TIMEOUT", "1.0"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
log = logging.getLogger("enrichment-consumer")

# ---------------------------------------------------------------------------
# Avro schemas
# ---------------------------------------------------------------------------

SCHEMA_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_schema(name: str) -> str:
    path = os.path.join(SCHEMA_DIR, name)
    with open(path) as f:
        return f.read()


RAW_SCHEMA_STR = _load_schema("raw_telemetry.avsc")
ENRICHED_SCHEMA_STR = _load_schema("enriched_telemetry.avsc")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown = threading.Event()


def _handle_signal(signum: int, _frame: Any) -> None:
    sig_name = signal.Signals(signum).name
    log.info("Received %s — initiating graceful shutdown", sig_name)
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# ---------------------------------------------------------------------------
# NetBox CDC Cache
# ---------------------------------------------------------------------------


class NetBoxCache:
    """
    In-memory cache populated from the netbox-changes Kafka topic.
    Keyed by device_name. Each entry contains device metadata and
    a dict of interface_name → interface metadata.
    """

    def __init__(self):
        self._devices: dict[str, dict[str, Any]] = {}
        self._interfaces: dict[str, dict[str, dict[str, Any]]] = {}  # device → {iface_name → meta}
        self._lock = threading.Lock()
        self._device_count = 0
        self._interface_count = 0

    def update_from_event(self, event: dict[str, Any]) -> None:
        """Update cache from a netbox-changes event."""
        device_name = event.get("device_name", "")
        if not device_name:
            return

        with self._lock:
            if event.get("event_type") == "deleted":
                self._devices.pop(device_name, None)
                self._interfaces.pop(device_name, None)
                log.debug("Cache: removed %s (deleted)", device_name)
                self._recount()
                return

            # Extract device-level metadata
            self._devices[device_name] = {
                "site_name": event.get("site_name"),
                "site_slug": event.get("site_slug"),
                "region": event.get("region"),
                "rack_name": event.get("rack_name"),
                "rack_position": event.get("rack_position"),
                "rack_face": event.get("rack_face"),
                "nb_device_role": event.get("device_role"),
                "device_type": event.get("device_type"),
                "device_platform": event.get("device_platform"),
                "device_serial": event.get("device_serial"),
                "primary_ip": event.get("primary_ip"),
                "tenant_name": event.get("tenant_name"),
                "tenant_slug": event.get("tenant_slug"),
                "device_tags": event.get("device_tags", []),
                "custom_fields": event.get("custom_fields"),
            }

            # Extract interface-level metadata
            iface_map: dict[str, dict[str, Any]] = {}
            for iface in event.get("interfaces", []):
                iface_map[iface["name"]] = {
                    "interface_type": iface.get("type"),
                    "interface_enabled": iface.get("enabled"),
                    "interface_mode": iface.get("mode"),
                    "interface_tags": iface.get("tags", []),
                    "cable_peer_device": iface.get("cable_peer_device"),
                    "cable_peer_interface": iface.get("cable_peer_interface"),
                    "cable_status": iface.get("cable_status"),
                    "cable_type": iface.get("cable_type"),
                }
            self._interfaces[device_name] = iface_map
            self._recount()

    def _recount(self):
        self._device_count = len(self._devices)
        self._interface_count = sum(len(v) for v in self._interfaces.values())

    def get_device(self, device_name: str) -> dict[str, Any] | None:
        with self._lock:
            return self._devices.get(device_name)

    def get_interface(self, device_name: str, interface_name: str) -> dict[str, Any] | None:
        with self._lock:
            device_ifaces = self._interfaces.get(device_name)
            if device_ifaces:
                return device_ifaces.get(interface_name)
            return None

    @property
    def device_count(self) -> int:
        return self._device_count

    @property
    def interface_count(self) -> int:
        return self._interface_count


_cache = NetBoxCache()

# ---------------------------------------------------------------------------
# CDC consumer (background thread)
# ---------------------------------------------------------------------------


def _bootstrap_and_watch_netbox(sr_client: SchemaRegistryClient) -> None:
    """
    Read the netbox-changes compacted topic from the beginning to build cache,
    then continue watching for live updates.
    """
    log.info("CDC: Starting netbox-changes consumer (bootstrap + watch)")

    # No consumer group — always replay from offset 0 to rebuild the in-memory cache.
    # Using assign() with explicit offsets instead of subscribe() so we don't depend
    # on committed offsets for a cache that only lives in memory.
    cdc_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.auto.commit": False,
    }
    cdc_consumer = Consumer(cdc_conf)

    # We need the netbox-changes schema for deserialization.
    # Use Schema Registry auto-detection (schema ID embedded in message).
    netbox_deser = AvroDeserializer(sr_client)

    # Get partition list and assign from offset 0
    cluster_meta = cdc_consumer.list_topics(NETBOX_TOPIC, timeout=10)
    topic_meta = cluster_meta.topics.get(NETBOX_TOPIC)
    if not topic_meta or not topic_meta.partitions:
        log.error("CDC: Topic %s not found or has no partitions", NETBOX_TOPIC)
        return

    partitions = [
        TopicPartition(NETBOX_TOPIC, p, OFFSET_BEGINNING)
        for p in topic_meta.partitions
    ]
    cdc_consumer.assign(partitions)
    log.info("CDC: Assigned %d partition(s) of %s from offset 0", len(partitions), NETBOX_TOPIC)

    bootstrap_done = False
    bootstrap_count = 0
    empty_polls = 0

    while not _shutdown.is_set():
        msg = cdc_consumer.poll(timeout=1.0)
        if msg is None:
            empty_polls += 1
            # After 5 empty polls during bootstrap, consider it done
            if not bootstrap_done and empty_polls >= 5:
                bootstrap_done = True
                log.info(
                    "CDC: Bootstrap complete — %d devices, %d interfaces cached",
                    _cache.device_count, _cache.interface_count,
                )
            continue

        empty_polls = 0

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                if not bootstrap_done:
                    bootstrap_done = True
                    log.info(
                        "CDC: Bootstrap complete (EOF) — %d devices, %d interfaces cached",
                        _cache.device_count, _cache.interface_count,
                    )
                continue
            log.error("CDC consumer error: %s", msg.error())
            continue

        try:
            event = netbox_deser(msg.value(), SerializationContext(NETBOX_TOPIC, MessageField.VALUE))
            if event:
                _cache.update_from_event(event)
                bootstrap_count += 1
                if not bootstrap_done and bootstrap_count % 10 == 0:
                    log.info("CDC: Bootstrap progress — %d events processed", bootstrap_count)
        except Exception as e:
            log.error("CDC: Failed to deserialize netbox-changes message: %s", e)

    cdc_consumer.close()
    log.info("CDC: Consumer closed")


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------


def enrich_event(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Build an enriched event dict from a raw event + CDC cache.
    No API calls — everything from local cache.
    """
    enriched: dict[str, Any] = dict(raw)
    enriched["enrichment_ts"] = int(time.time() * 1000)
    enriched["enriched"] = False

    # Set defaults for all enrichment fields
    for field in (
        "site_name", "site_slug", "region",
        "rack_name", "rack_position", "rack_face",
        "nb_device_role", "device_type", "device_platform", "device_serial",
        "primary_ip", "tenant_name", "tenant_slug",
        "cable_peer_device", "cable_peer_interface", "cable_status", "cable_type",
        "interface_type", "interface_enabled", "interface_mode",
        "custom_fields",
    ):
        enriched.setdefault(field, None)
    enriched.setdefault("device_tags", [])
    enriched.setdefault("interface_tags", [])

    device_name = raw.get("device_name", "")
    interface_name = raw.get("interface_name", "")

    # Lookup from CDC cache
    dev_meta = _cache.get_device(device_name)
    if dev_meta:
        enriched.update(dev_meta)
        enriched["enriched"] = True

    iface_meta = _cache.get_interface(device_name, interface_name)
    if iface_meta:
        enriched.update(iface_meta)

    return enriched


# ---------------------------------------------------------------------------
# Kafka wiring
# ---------------------------------------------------------------------------


def _build_consumer(sr_client: SchemaRegistryClient) -> tuple:
    avro_deser = AvroDeserializer(sr_client, RAW_SCHEMA_STR)
    key_deser = StringDeserializer("utf_8")

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,
    }
    consumer = Consumer(conf)
    return consumer, key_deser, avro_deser


def _build_producer(sr_client: SchemaRegistryClient) -> tuple:
    avro_ser = AvroSerializer(sr_client, ENRICHED_SCHEMA_STR)
    key_ser = StringSerializer("utf_8")

    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 50,
        "batch.num.messages": 500,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
    }
    producer = Producer(conf)
    return producer, key_ser, avro_ser


def _delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    log.info("Starting enrichment consumer (v2 — CDC-backed)")
    log.info(
        "Config — bootstrap=%s schema_registry=%s group=%s",
        KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL, CONSUMER_GROUP,
    )
    log.info(
        "Topics — raw=%s enriched=%s netbox_cdc=%s",
        RAW_TOPIC, ENRICHED_TOPIC, NETBOX_TOPIC,
    )

    # --- Schema Registry client ---
    sr_conf = {"url": SCHEMA_REGISTRY_URL}
    sr_client = SchemaRegistryClient(sr_conf)

    # --- Start CDC background thread ---
    cdc_thread = threading.Thread(
        target=_bootstrap_and_watch_netbox,
        args=(sr_client,),
        daemon=True,
        name="cdc-consumer",
    )
    cdc_thread.start()

    # Wait for bootstrap to get at least some data (up to 30s)
    log.info("Waiting for CDC bootstrap...")
    for _ in range(30):
        if _cache.device_count > 0 or _shutdown.is_set():
            break
        time.sleep(1)
    log.info(
        "CDC cache ready: %d devices, %d interfaces",
        _cache.device_count, _cache.interface_count,
    )

    # --- Kafka consumer & producer ---
    consumer, key_deser, avro_deser = _build_consumer(sr_client)
    producer, key_ser, avro_ser = _build_producer(sr_client)

    consumer.subscribe([RAW_TOPIC])
    log.info("Subscribed to topic: %s", RAW_TOPIC)

    msg_count = 0
    enriched_count = 0
    last_log_ts = time.monotonic()

    try:
        while not _shutdown.is_set():
            msg = consumer.poll(timeout=POLL_TIMEOUT)
            if msg is None:
                producer.flush(0.1)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # Deserialize
            try:
                raw_value = avro_deser(
                    msg.value(),
                    SerializationContext(RAW_TOPIC, MessageField.VALUE),
                )
            except Exception as exc:
                log.error("Avro deserialization failed, skipping: %s", exc)
                consumer.commit(message=msg, asynchronous=False)
                continue

            if raw_value is None:
                consumer.commit(message=msg, asynchronous=False)
                continue

            # Enrich (from CDC cache — no API calls)
            enriched = enrich_event(raw_value)
            if enriched.get("enriched"):
                enriched_count += 1

            # Build key
            key_str = f"{enriched.get('device_name', '')}::{enriched.get('interface_name', '')}"

            # Serialize and produce
            try:
                producer.produce(
                    topic=ENRICHED_TOPIC,
                    key=key_ser(key_str, SerializationContext(ENRICHED_TOPIC, MessageField.KEY)),
                    value=avro_ser(enriched, SerializationContext(ENRICHED_TOPIC, MessageField.VALUE)),
                    on_delivery=_delivery_report,
                )
                producer.poll(0)
            except Exception as exc:
                log.error("Failed to produce enriched event: %s", exc)
                continue

            consumer.commit(message=msg, asynchronous=False)
            msg_count += 1

            # Periodic stats
            now = time.monotonic()
            if now - last_log_ts >= 60:
                log.info(
                    "Stats — processed=%d enriched=%d cache_devices=%d cache_interfaces=%d",
                    msg_count, enriched_count, _cache.device_count, _cache.interface_count,
                )
                last_log_ts = now

    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        raise
    finally:
        log.info("Shutting down — flushing producer…")
        producer.flush(timeout=10)
        log.info("Closing consumer (processed %d messages, %d enriched)…", msg_count, enriched_count)
        consumer.close()
        log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
