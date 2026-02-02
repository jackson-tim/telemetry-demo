#!/usr/bin/env python3
"""
Mock gNMI telemetry producer for network interface counters.

Simulates a Clos fabric (4 spines + 24 leaves) with ~48 interfaces each,
producing Avro-serialized telemetry to Kafka every INTERVAL seconds.

Counters are monotonically increasing with realistic rates. Anomalies
(CRC spikes, interface flaps, traffic bursts) are injected stochastically.

Environment:
    KAFKA_BOOTSTRAP     - Kafka broker(s)          default: kafka:9092
    SCHEMA_REGISTRY_URL - Confluent Schema Registry default: http://schema-registry:8081
    KAFKA_TOPIC         - Target topic              default: interface-telemetry-raw
    INTERVAL            - Seconds between polls     default: 30
    LOG_LEVEL           - Python log level           default: INFO
    ANOMALY_PROBABILITY - Per-device anomaly chance  default: 0.02
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from confluent_kafka import Producer
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

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "interface-telemetry-raw")
INTERVAL = int(os.getenv("INTERVAL", "30"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ANOMALY_PROBABILITY = float(os.getenv("ANOMALY_PROBABILITY", "0.02"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("mock-producer")

# ---------------------------------------------------------------------------
# Fabric topology definition
# ---------------------------------------------------------------------------

SPINE_COUNT = 4
LEAF_COUNT = 24

# Interface templates per role
# Spines: 32x400G fabric + 16x100G fabric = 48
# Leaves: 24x400G uplinks + 24x100G server-facing = 48
SPINE_INTERFACES = (
    [{"prefix": "et-0/0/", "start": 0, "count": 32, "speed": 400_000, "mtu": 9216, "desc_tpl": "to-leaf-{i:02d}"}] +
    [{"prefix": "et-0/1/", "start": 0, "count": 16, "speed": 100_000, "mtu": 9216, "desc_tpl": "to-border-{i:02d}"}]
)
LEAF_INTERFACES = (
    [{"prefix": "et-0/0/", "start": 0, "count": 4, "speed": 400_000, "mtu": 9216, "desc_tpl": "to-spine-{i:02d}"}] +
    [{"prefix": "et-0/1/", "start": 0, "count": 44, "speed": 100_000, "mtu": 9216, "desc_tpl": "server-{i:03d}"}]
)


@dataclass
class InterfaceSimState:
    """Mutable simulation state for a single interface."""

    name: str
    description: Optional[str]
    speed: int  # Mbps
    mtu: int
    admin_up: bool = True
    oper_up: bool = True

    # Monotonic counters
    in_octets: int = 0
    out_octets: int = 0
    in_errors: int = 0
    out_errors: int = 0
    in_discards: int = 0
    out_discards: int = 0
    in_unicast_pkts: int = 0
    out_unicast_pkts: int = 0
    in_crc_errors: int = 0
    in_fcs_errors: int = 0

    # Baseline traffic rate (bytes/sec) — set during init
    base_rate_bps: int = 0

    # Flap state
    _flap_remaining: int = 0


@dataclass
class DeviceSimState:
    """Mutable simulation state for a network device."""

    name: str
    role: str  # spine | leaf
    vendor: str
    interfaces: list[InterfaceSimState] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Fabric builder
# ---------------------------------------------------------------------------


def _build_interfaces(templates: list[dict], device_role: str) -> list[InterfaceSimState]:
    """Create InterfaceSimState objects from interface templates."""
    ifaces: list[InterfaceSimState] = []
    for tpl in templates:
        for i in range(tpl["count"]):
            idx = tpl["start"] + i
            name = f"{tpl['prefix']}{idx}"
            desc = tpl["desc_tpl"].format(i=idx) if tpl.get("desc_tpl") else None

            # Realistic baseline: fabric links run 30-70% util, server links 10-50%
            speed_bytes = tpl["speed"] * 125_000  # Mbps → bytes/sec
            if "spine" in tpl.get("desc_tpl", "") or "border" in tpl.get("desc_tpl", ""):
                utilization = random.uniform(0.30, 0.70)
            else:
                utilization = random.uniform(0.10, 0.50)

            base_rate = int(speed_bytes * utilization)

            iface = InterfaceSimState(
                name=name,
                description=desc,
                speed=tpl["speed"],
                mtu=tpl["mtu"],
                base_rate_bps=base_rate,
            )
            # Seed counters as if device has been up for 1-30 days
            uptime_seconds = random.randint(86_400, 86_400 * 30)
            iface.in_octets = int(base_rate * uptime_seconds * random.uniform(0.9, 1.1))
            iface.out_octets = int(base_rate * uptime_seconds * random.uniform(0.9, 1.1))
            avg_pkt_size = random.randint(800, 1400)
            iface.in_unicast_pkts = iface.in_octets // avg_pkt_size
            iface.out_unicast_pkts = iface.out_octets // avg_pkt_size
            # Tiny baseline error counts
            iface.in_errors = random.randint(0, 50)
            iface.out_errors = random.randint(0, 20)
            iface.in_discards = random.randint(0, 100)
            iface.out_discards = random.randint(0, 30)
            iface.in_crc_errors = random.randint(0, 5)
            iface.in_fcs_errors = random.randint(0, 3)

            ifaces.append(iface)
    return ifaces


def build_fabric() -> list[DeviceSimState]:
    """Build the full simulated Clos fabric."""
    devices: list[DeviceSimState] = []
    vendors = ["juniper", "arista", "nokia"]

    for i in range(1, SPINE_COUNT + 1):
        dev = DeviceSimState(
            name=f"spine-{i:02d}",
            role="spine",
            vendor=random.choice(vendors),
            interfaces=_build_interfaces(SPINE_INTERFACES, "spine"),
        )
        devices.append(dev)
        log.info("Built %s with %d interfaces (vendor=%s)", dev.name, len(dev.interfaces), dev.vendor)

    for i in range(1, LEAF_COUNT + 1):
        dev = DeviceSimState(
            name=f"leaf-{i:02d}",
            role="leaf",
            vendor=random.choice(vendors),
            interfaces=_build_interfaces(LEAF_INTERFACES, "leaf"),
        )
        devices.append(dev)
        log.info("Built %s with %d interfaces (vendor=%s)", dev.name, len(dev.interfaces), dev.vendor)

    total_ifaces = sum(len(d.interfaces) for d in devices)
    log.info("Fabric ready: %d devices, %d total interfaces", len(devices), total_ifaces)
    return devices


# ---------------------------------------------------------------------------
# Counter evolution + anomaly injection
# ---------------------------------------------------------------------------


def _evolve_counters(iface: InterfaceSimState, interval: int) -> None:
    """Advance counters by a realistic increment for the given interval."""
    if not iface.oper_up:
        # Interface is down — no traffic
        return

    # Normal traffic with ±15% jitter
    jitter = random.uniform(0.85, 1.15)
    bytes_in = int(iface.base_rate_bps * interval * jitter)
    bytes_out = int(iface.base_rate_bps * interval * random.uniform(0.85, 1.15))

    iface.in_octets += bytes_in
    iface.out_octets += bytes_out

    avg_pkt = random.randint(800, 1400)
    iface.in_unicast_pkts += bytes_in // avg_pkt
    iface.out_unicast_pkts += bytes_out // avg_pkt

    # Very low probability of normal errors (1 in 10,000 polls)
    if random.random() < 0.0001:
        iface.in_errors += random.randint(1, 3)
    if random.random() < 0.00005:
        iface.out_errors += 1
    if random.random() < 0.0002:
        iface.in_discards += random.randint(1, 5)


def inject_anomalies(device: DeviceSimState, interval: int) -> None:
    """Stochastically inject anomalies on a device's interfaces."""
    if random.random() > ANOMALY_PROBABILITY:
        return

    anomaly_type = random.choices(
        ["crc_spike", "flap", "traffic_spike", "discard_burst"],
        weights=[0.30, 0.25, 0.30, 0.15],
        k=1,
    )[0]

    # Pick a random interface
    target = random.choice(device.interfaces)

    if anomaly_type == "crc_spike":
        spike = random.randint(50, 500)
        target.in_crc_errors += spike
        target.in_errors += spike
        log.warning(
            "ANOMALY crc_spike on %s %s: +%d CRC errors",
            device.name, target.name, spike,
        )

    elif anomaly_type == "flap":
        if target.admin_up and target.oper_up:
            target._flap_remaining = random.randint(2, 6)  # flap for N polls
            target.oper_up = False
            log.warning(
                "ANOMALY flap on %s %s: interface DOWN (will recover in %d polls)",
                device.name, target.name, target._flap_remaining,
            )

    elif anomaly_type == "traffic_spike":
        multiplier = random.uniform(3.0, 10.0)
        spike_bytes = int(target.base_rate_bps * interval * multiplier)
        target.in_octets += spike_bytes
        target.out_octets += int(spike_bytes * random.uniform(0.5, 1.5))
        avg_pkt = random.randint(64, 512)  # small packets during spike
        target.in_unicast_pkts += spike_bytes // avg_pkt
        target.out_unicast_pkts += spike_bytes // avg_pkt
        log.warning(
            "ANOMALY traffic_spike on %s %s: %.1fx burst",
            device.name, target.name, multiplier,
        )

    elif anomaly_type == "discard_burst":
        discards = random.randint(100, 2000)
        target.in_discards += discards
        log.warning(
            "ANOMALY discard_burst on %s %s: +%d discards",
            device.name, target.name, discards,
        )


def recover_flaps(device: DeviceSimState) -> None:
    """Tick down flap timers and recover interfaces."""
    for iface in device.interfaces:
        if iface._flap_remaining > 0:
            iface._flap_remaining -= 1
            if iface._flap_remaining == 0:
                iface.oper_up = True
                log.info("RECOVERY %s %s: interface back UP", device.name, iface.name)


# ---------------------------------------------------------------------------
# Avro record builder
# ---------------------------------------------------------------------------


def build_record(device: DeviceSimState, iface: InterfaceSimState, now_ms: int) -> dict[str, Any]:
    """Build an Avro-compatible dict from device + interface state."""
    # Determine if vendor supports CRC/FCS counters
    has_crc = device.vendor in ("juniper", "nokia")
    has_fcs = device.vendor in ("juniper",)

    return {
        "timestamp": now_ms,
        "device_name": device.name,
        "device_role": device.role,
        "interface_name": iface.name,
        "interface_description": iface.description,
        "counters": {
            "in_octets": iface.in_octets,
            "out_octets": iface.out_octets,
            "in_errors": iface.in_errors,
            "out_errors": iface.out_errors,
            "in_discards": iface.in_discards,
            "out_discards": iface.out_discards,
            "in_unicast_pkts": iface.in_unicast_pkts,
            "out_unicast_pkts": iface.out_unicast_pkts,
            "in_crc_errors": iface.in_crc_errors if has_crc else None,
            "in_fcs_errors": iface.in_fcs_errors if has_fcs else None,
        },
        "state": {
            "admin_status": "UP" if iface.admin_up else "DOWN",
            "oper_status": "UP" if iface.oper_up else "DOWN",
            "speed": iface.speed if iface.oper_up else 0,
            "mtu": iface.mtu,
        },
        "metadata": {
            "source": "mock",
            "vendor": device.vendor,
            "collection_timestamp": now_ms,
        },
    }


# ---------------------------------------------------------------------------
# Kafka / Schema Registry setup
# ---------------------------------------------------------------------------


def load_schema() -> str:
    """Load the Avro schema from the .avsc file adjacent to this script."""
    schema_path = Path(__file__).parent / "interface_telemetry.avsc"
    schema_str = schema_path.read_text()
    # Validate it's valid JSON
    json.loads(schema_str)
    log.info("Loaded Avro schema from %s", schema_path)
    return schema_str


def create_producer(schema_str: str) -> tuple[Producer, AvroSerializer, StringSerializer]:
    """Create the Kafka producer and Avro serializer (registers schema)."""
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        conf={"auto.register.schemas": True},
    )
    log.info("Schema registered with Schema Registry at %s", SCHEMA_REGISTRY_URL)

    kafka_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "mock-telemetry-producer",
        "linger.ms": 100,
        "batch.num.messages": 500,
        "compression.type": "lz4",
        "acks": "1",
    }
    producer = Producer(kafka_conf)
    log.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP)

    key_serializer = StringSerializer("utf_8")
    return producer, avro_serializer, key_serializer


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

_delivery_ok = 0
_delivery_fail = 0


def _on_delivery(err, msg):
    global _delivery_ok, _delivery_fail
    if err:
        _delivery_fail += 1
        if _delivery_fail % 100 == 1:
            log.error("Delivery failed: %s", err)
    else:
        _delivery_ok += 1


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

_running = True


def _shutdown(signum, frame):
    global _running
    log.info("Received signal %s, shutting down…", signum)
    _running = False


def main() -> None:
    global _delivery_ok, _delivery_fail

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("=== Mock gNMI Telemetry Producer ===")
    log.info("Kafka:    %s", KAFKA_BOOTSTRAP)
    log.info("Registry: %s", SCHEMA_REGISTRY_URL)
    log.info("Topic:    %s", KAFKA_TOPIC)
    log.info("Interval: %ds", INTERVAL)
    log.info("Anomaly:  %.1f%% per device per poll", ANOMALY_PROBABILITY * 100)

    # Build fabric
    fabric = build_fabric()

    # Set up Kafka
    schema_str = load_schema()
    producer, avro_ser, key_ser = create_producer(schema_str)

    poll_count = 0
    while _running:
        poll_start = time.time()
        poll_count += 1
        now_ms = int(time.time() * 1000)
        records_sent = 0

        for device in fabric:
            # Advance counters
            for iface in device.interfaces:
                _evolve_counters(iface, INTERVAL)

            # Inject anomalies
            inject_anomalies(device, INTERVAL)

            # Recover flapping interfaces
            recover_flaps(device)

            # Produce records
            for iface in device.interfaces:
                record = build_record(device, iface, now_ms)
                key = f"{device.name}:{iface.name}"

                try:
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=key_ser(key),
                        value=avro_ser(
                            record,
                            SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
                        ),
                        on_delivery=_on_delivery,
                    )
                    records_sent += 1
                except BufferError:
                    # Local queue full — flush and retry
                    producer.flush(timeout=5.0)
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=key_ser(key),
                        value=avro_ser(
                            record,
                            SerializationContext(KAFKA_TOPIC, MessageField.VALUE),
                        ),
                        on_delivery=_on_delivery,
                    )
                    records_sent += 1

                # Drain callbacks periodically to avoid buffer pressure
                if records_sent % 200 == 0:
                    producer.poll(0)

        # Final flush for this poll cycle
        producer.flush(timeout=10.0)
        elapsed = time.time() - poll_start

        log.info(
            "Poll %d: sent %d records in %.1fs (ok=%d fail=%d)",
            poll_count, records_sent, elapsed, _delivery_ok, _delivery_fail,
        )

        # Reset delivery counters each poll for cleaner logging
        _delivery_ok = 0
        _delivery_fail = 0

        # Sleep for remaining interval
        sleep_time = max(0, INTERVAL - elapsed)
        if sleep_time > 0 and _running:
            log.debug("Sleeping %.1fs until next poll", sleep_time)
            # Sleep in 1s increments so we can respond to signals
            slept = 0.0
            while slept < sleep_time and _running:
                time.sleep(min(1.0, sleep_time - slept))
                slept += 1.0

    # Graceful shutdown
    log.info("Flushing remaining messages…")
    producer.flush(timeout=30.0)
    log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
