#!/usr/bin/env python3
"""
gNMI → Avro Normalizer
=======================
Consumes raw gNMI events from gnmic (JSON event format) on the
`gnmi-telemetry-raw` topic, transforms them into the canonical
InterfaceTelemetryEvent Avro schema, and produces to `interface-telemetry-raw`
so the existing enrichment pipeline picks them up.

gnmic event format (with split-events):
  Each message is a JSON object like:
  {
    "name": "interface_stats",
    "timestamp": 1706886000000000000,  // nanoseconds
    "tags": {
      "source": "srlinux-local",
      "interface_name": "ethernet-1/1",
      ...
    },
    "values": {
      "/interface/statistics/in-octets": 123456,
      ...
    }
  }

This normalizer accumulates events per (device, interface) within a time
window, then emits a complete InterfaceTelemetryEvent record.

Env vars:
    KAFKA_BOOTSTRAP       - Kafka broker(s)          (default: 10.42.201.20:9092)
    SCHEMA_REGISTRY_URL   - Schema Registry           (default: http://10.42.201.20:8081)
    CONSUMER_GROUP        - Consumer group id          (default: gnmi-normalizer)
    LOG_LEVEL             - Logging level              (default: INFO)
    GNMI_RAW_TOPIC        - Source topic               (default: gnmi-telemetry-raw)
    AVRO_RAW_TOPIC        - Destination topic          (default: interface-telemetry-raw)
    FLUSH_INTERVAL        - Seconds to buffer events   (default: 15)
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import time
import threading
from collections import defaultdict
from typing import Any

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "10.42.201.20:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://10.42.201.20:8081")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "gnmi-normalizer")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
GNMI_RAW_TOPIC = os.environ.get("GNMI_RAW_TOPIC", "gnmi-telemetry-raw")
AVRO_RAW_TOPIC = os.environ.get("AVRO_RAW_TOPIC", "interface-telemetry-raw")
FLUSH_INTERVAL = float(os.environ.get("FLUSH_INTERVAL", "15"))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
log = logging.getLogger("gnmi-normalizer")

# ---------------------------------------------------------------------------
# Avro schema (must match interface-telemetry-raw-value in Schema Registry)
# ---------------------------------------------------------------------------

RAW_SCHEMA_STR = """{
  "type": "record",
  "name": "InterfaceTelemetryEvent",
  "namespace": "net.idge.telemetry.interface",
  "fields": [
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "device_name", "type": "string"},
    {"name": "device_role", "type": {"type": "enum", "name": "DeviceRole", "symbols": ["spine", "leaf", "border", "superspine", "dcgw", "unknown"]}},
    {"name": "interface_name", "type": "string"},
    {"name": "interface_description", "type": ["null", "string"], "default": null},
    {"name": "counters", "type": {"type": "record", "name": "InterfaceCounters", "fields": [
      {"name": "in_octets", "type": "long"},
      {"name": "out_octets", "type": "long"},
      {"name": "in_errors", "type": "long"},
      {"name": "out_errors", "type": "long"},
      {"name": "in_discards", "type": "long"},
      {"name": "out_discards", "type": "long"},
      {"name": "in_unicast_pkts", "type": "long"},
      {"name": "out_unicast_pkts", "type": "long"},
      {"name": "in_crc_errors", "type": ["null", "long"], "default": null},
      {"name": "in_fcs_errors", "type": ["null", "long"], "default": null}
    ]}},
    {"name": "state", "type": {"type": "record", "name": "InterfaceState", "fields": [
      {"name": "admin_status", "type": {"type": "enum", "name": "AdminStatus", "symbols": ["UP", "DOWN"]}},
      {"name": "oper_status", "type": {"type": "enum", "name": "OperStatus", "symbols": ["UP", "DOWN"]}},
      {"name": "speed", "type": "long"},
      {"name": "mtu", "type": "int"}
    ]}},
    {"name": "metadata", "type": {"type": "record", "name": "CollectionMetadata", "fields": [
      {"name": "source", "type": {"type": "enum", "name": "CollectionSource", "symbols": ["gnmic", "snmp", "mock", "openconfig", "native"]}},
      {"name": "vendor", "type": {"type": "enum", "name": "Vendor", "symbols": ["juniper", "arista", "nokia", "cisco", "sonic", "cumulus", "mock", "unknown"]}},
      {"name": "collection_timestamp", "type": "long", "logicalType": "timestamp-millis"}
    ]}}
  ]
}"""

# ---------------------------------------------------------------------------
# Nokia SR Linux gNMI path → field mapping
# ---------------------------------------------------------------------------

# SRL interface name normalization: ethernet-1/1 → ethernet-1/1 (already clean)
# But we strip the srl_nokia-interfaces: prefix if present

# Map from gnmic event value keys to our counter/state fields
COUNTER_MAP = {
    # SRL /interface/statistics counters
    "in-octets": "in_octets",
    "out-octets": "out_octets",
    "in-error-packets": "in_errors",
    "out-error-packets": "out_errors",
    "in-discarded-packets": "in_discards",
    "out-discarded-packets": "out_discards",
    "in-unicast-packets": "in_unicast_pkts",
    "out-unicast-packets": "out_unicast_pkts",
    "in-fcs-error-packets": "in_fcs_errors",
    # SRL /interface/ethernet/statistics
    "in-crc-error-frames": "in_crc_errors",
    # Alternate names (other vendors / SRL versions)
    "in-errors": "in_errors",
    "out-errors": "out_errors",
    "in-discards": "in_discards",
    "out-discards": "out_discards",
    "in-fcs-errors": "in_fcs_errors",
}

STATE_MAP = {
    "oper-state": "oper_status",
    "admin-state": "admin_status",
}

# Speed mapping for SRL interface types
SRL_SPEED_MAP = {
    "mgmt0": 1_000,       # 1G management
    "ethernet-1/": 100_000,  # default 100G, will be overridden by actual speed
    "lo0": 0,
}

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown = threading.Event()


def _handle_signal(signum: int, _frame: Any) -> None:
    log.info("Received signal %s — shutting down", signum)
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ---------------------------------------------------------------------------
# Event accumulator
# ---------------------------------------------------------------------------


class InterfaceAccumulator:
    """
    Accumulates gnmic split events for a (device, interface) pair
    and produces a complete InterfaceTelemetryEvent when flushed.
    """

    def __init__(self):
        # Key: (device_name, interface_name) → accumulated values
        self._buffer: dict[tuple[str, str], dict[str, Any]] = defaultdict(
            lambda: {
                "timestamp_ms": 0,
                "counters": {},
                "state": {},
                "description": None,
                "mtu": 9216,
                "speed": 0,
                "ifindex": None,
            }
        )
        self._lock = threading.Lock()

    def add_event(self, event: dict[str, Any]) -> None:
        """Process a single gnmic event and accumulate values."""
        tags = event.get("tags", {})
        values = event.get("values", {})

        # Extract device (source target) and interface
        device_name = tags.get("source", "unknown")
        interface_name = tags.get("interface_name", "")

        if not interface_name:
            # Try to extract from subscription path tags
            for key in tags:
                if "name" in key.lower() and key != "source":
                    interface_name = tags[key]
                    break

        if not interface_name:
            return

        # Convert gnmic nanosecond timestamp to milliseconds
        ts_ns = event.get("timestamp", 0)
        ts_ms = ts_ns // 1_000_000 if ts_ns > 1e15 else ts_ns  # handle both ns and ms

        key = (device_name, interface_name)

        with self._lock:
            acc = self._buffer[key]
            acc["timestamp_ms"] = max(acc["timestamp_ms"], ts_ms)

            for vkey, vval in values.items():
                # Strip path prefixes to get the leaf name
                leaf = vkey.rsplit("/", 1)[-1] if "/" in vkey else vkey
                # Remove vendor prefixes
                leaf = re.sub(r'^srl_nokia-interfaces:', '', leaf)

                # Counter mapping
                if leaf in COUNTER_MAP:
                    if isinstance(vval, (int, float)):
                        acc["counters"][COUNTER_MAP[leaf]] = int(vval)

                # State mapping
                elif leaf in STATE_MAP:
                    field = STATE_MAP[leaf]
                    if isinstance(vval, str):
                        # SRL uses enable/disable, normalize to UP/DOWN
                        val_upper = vval.upper()
                        if val_upper in ("ENABLE", "UP"):
                            acc["state"][field] = "UP"
                        elif val_upper in ("DISABLE", "DOWN"):
                            acc["state"][field] = "DOWN"
                        else:
                            acc["state"][field] = val_upper

                # MTU
                elif leaf == "mtu":
                    if isinstance(vval, (int, float)):
                        acc["mtu"] = int(vval)

                # Speed (SRL reports in Mbps or as string like "100G")
                elif leaf in ("port-speed", "speed"):
                    acc["speed"] = _parse_speed(vval)

                # ifindex
                elif leaf == "ifindex":
                    acc["ifindex"] = vval

                # Description
                elif leaf == "description":
                    acc["description"] = str(vval) if vval else None

                # Last change
                elif leaf == "last-change":
                    pass  # informational, skip for now

    def flush(self) -> list[dict[str, Any]]:
        """Flush all accumulated interfaces into Avro-compatible records."""
        records = []
        now_ms = int(time.time() * 1000)

        with self._lock:
            for (device_name, interface_name), acc in self._buffer.items():
                ts = acc["timestamp_ms"] if acc["timestamp_ms"] > 0 else now_ms

                # Build the Avro record
                record = {
                    "timestamp": ts,
                    "device_name": device_name,
                    "device_role": "unknown",  # enricher will fill from NetBox
                    "interface_name": interface_name,
                    "interface_description": acc.get("description"),
                    "counters": {
                        "in_octets": acc["counters"].get("in_octets", 0),
                        "out_octets": acc["counters"].get("out_octets", 0),
                        "in_errors": acc["counters"].get("in_errors", 0),
                        "out_errors": acc["counters"].get("out_errors", 0),
                        "in_discards": acc["counters"].get("in_discards", 0),
                        "out_discards": acc["counters"].get("out_discards", 0),
                        "in_unicast_pkts": acc["counters"].get("in_unicast_pkts", 0),
                        "out_unicast_pkts": acc["counters"].get("out_unicast_pkts", 0),
                        "in_crc_errors": acc["counters"].get("in_crc_errors"),
                        "in_fcs_errors": acc["counters"].get("in_fcs_errors"),
                    },
                    "state": {
                        "admin_status": acc["state"].get("admin_status", "UP"),
                        "oper_status": acc["state"].get("oper_status", "UP"),
                        "speed": acc.get("speed", 0),
                        "mtu": acc.get("mtu", 9216),
                    },
                    "metadata": {
                        "source": "gnmic",
                        "vendor": "nokia",
                        "collection_timestamp": now_ms,
                    },
                }
                records.append(record)

            self._buffer.clear()

        return records


def _parse_speed(val: Any) -> int:
    """Parse speed value into Mbps."""
    if isinstance(val, (int, float)):
        v = int(val)
        # If value is very large, it's probably in bps
        if v > 1_000_000:
            return v // 1_000_000
        return v
    if isinstance(val, str):
        val = val.upper().strip()
        m = re.match(r'(\d+)\s*([GMK])?', val)
        if m:
            num = int(m.group(1))
            unit = m.group(2)
            if unit == 'G':
                return num * 1_000
            elif unit == 'K':
                return num // 1_000
            return num
    return 0


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    log.info("=== gNMI → Avro Normalizer ===")
    log.info("Kafka:    %s", KAFKA_BOOTSTRAP)
    log.info("Registry: %s", SCHEMA_REGISTRY_URL)
    log.info("Input:    %s → Output: %s", GNMI_RAW_TOPIC, AVRO_RAW_TOPIC)
    log.info("Flush:    every %.0fs", FLUSH_INTERVAL)

    # Schema Registry + Avro serializer
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_ser = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=RAW_SCHEMA_STR,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )
    key_ser = StringSerializer("utf_8")

    # Consumer (JSON from gnmic)
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([GNMI_RAW_TOPIC])
    log.info("Subscribed to: %s", GNMI_RAW_TOPIC)

    # Producer (Avro)
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "gnmi-normalizer",
        "linger.ms": 50,
        "batch.num.messages": 500,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
    }
    producer = Producer(producer_conf)

    accumulator = InterfaceAccumulator()
    last_flush = time.monotonic()
    events_consumed = 0
    records_produced = 0

    def _delivery_report(err, msg):
        nonlocal records_produced
        if err:
            log.error("Delivery failed: %s", err)
        else:
            records_produced += 1

    try:
        while not _shutdown.is_set():
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("Consumer error: %s", msg.error())
                    continue

                # Deserialize JSON event from gnmic
                try:
                    raw_bytes = msg.value()
                    event = json.loads(raw_bytes.decode("utf-8"))
                    if events_consumed == 0:
                        log.info("First event received: %s", json.dumps(event)[:200])
                    accumulator.add_event(event)
                    events_consumed += 1
                    if events_consumed % 100 == 0:
                        log.info("Consumed %d events so far", events_consumed)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    log.warning("Failed to parse gnmic event: %s", e)
                    continue

            # Flush on interval
            now = time.monotonic()
            if now - last_flush >= FLUSH_INTERVAL:
                records = accumulator.flush()
                for record in records:
                    key = f"{record['device_name']}:{record['interface_name']}"
                    try:
                        producer.produce(
                            topic=AVRO_RAW_TOPIC,
                            key=key_ser(key, SerializationContext(AVRO_RAW_TOPIC, MessageField.KEY)),
                            value=avro_ser(record, SerializationContext(AVRO_RAW_TOPIC, MessageField.VALUE)),
                            on_delivery=_delivery_report,
                        )
                    except Exception as e:
                        log.error("Failed to produce record for %s: %s", key, e)

                producer.flush(timeout=5.0)

                log.info(
                    "Flush: %d gnmic events → %d Avro records (total consumed=%d produced=%d)",
                    events_consumed, len(records), events_consumed, records_produced,
                )

                last_flush = now

            producer.poll(0)

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush
        records = accumulator.flush()
        for record in records:
            key = f"{record['device_name']}:{record['interface_name']}"
            try:
                producer.produce(
                    topic=AVRO_RAW_TOPIC,
                    key=key_ser(key, SerializationContext(AVRO_RAW_TOPIC, MessageField.KEY)),
                    value=avro_ser(record, SerializationContext(AVRO_RAW_TOPIC, MessageField.VALUE)),
                    on_delivery=_delivery_report,
                )
            except Exception:
                pass
        producer.flush(timeout=10)
        consumer.close()
        log.info("Shutdown complete. Consumed=%d Produced=%d", events_consumed, records_produced)


if __name__ == "__main__":
    main()
