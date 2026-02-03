#!/usr/bin/env python3
"""
Flow Enrichment Consumer
========================
Reads raw flow records from sfacctd (via Kafka), enriches with:
  1. ifindex → interface_name (from gNMI-sourced compacted topic)
  2. interface_name → NetBox metadata (from netbox-changes compacted topic)
  3. BGP community → interconnection type classification

Produces enriched flow records to flow-telemetry-enriched topic.

Env vars:
    KAFKA_BOOTSTRAP       - Kafka broker(s)  (default: kafka:9092)
    CONSUMER_GROUP        - Consumer group    (default: flow-enrichment)
    RAW_TOPIC             - Source topic       (default: flow-telemetry-raw)
    ENRICHED_TOPIC        - Destination topic  (default: flow-telemetry-enriched)
    IFINDEX_TOPIC         - ifindex map topic  (default: ifindex-map)
    NETBOX_TOPIC          - NetBox CDC topic   (default: netbox-changes)
    LOG_LEVEL             - Logging level      (default: INFO)
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
from typing import Any

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException, TopicPartition, OFFSET_BEGINNING

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "flow-enrichment")
RAW_TOPIC = os.environ.get("RAW_TOPIC", "flow-telemetry-raw")
ENRICHED_TOPIC = os.environ.get("ENRICHED_TOPIC", "flow-telemetry-enriched")
IFINDEX_TOPIC = os.environ.get("IFINDEX_TOPIC", "ifindex-map")
NETBOX_TOPIC = os.environ.get("NETBOX_TOPIC", "netbox-changes")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# BGP community → interconnection type mapping
# Format: "ASN:VALUE" → type string
# Customize for your network's community scheme
COMMUNITY_MAP = {
    "65000:100": "transit",
    "65000:200": "private-peering",
    "65000:300": "ix-peering",
    "65000:400": "customer",
}

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
log = logging.getLogger("flow-enrichment")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

_shutdown = threading.Event()


def _handle_signal(signum, _frame):
    log.info("Received %s — shutting down", signal.Signals(signum).name)
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

# ---------------------------------------------------------------------------
# ifindex Cache
# ---------------------------------------------------------------------------


class IfIndexCache:
    """
    Maps (router_ip, ifindex) → interface_name.
    Populated from the ifindex-map compacted Kafka topic (sourced by gNMIc).
    """

    def __init__(self):
        self._map: dict[str, str] = {}  # "router:ifindex" → interface_name
        self._lock = threading.Lock()

    def update(self, router_ip: str, ifindex: int, interface_name: str) -> None:
        key = f"{router_ip}:{ifindex}"
        with self._lock:
            self._map[key] = interface_name

    def update_from_gnmic_event(self, event: dict) -> None:
        """Parse a gnmic event-format message to extract ifindex mappings."""
        # gnmic event format: {"name": "...", "timestamp": ..., "tags": {"interface_name": "ethernet-1/1", ...}, "values": {"ifindex": 12345}}
        tags = event.get("tags", {})
        values = event.get("values", {})
        source = tags.get("source", "")

        interface_name = tags.get("interface_name", "")
        if not interface_name:
            # Try to extract from path
            name_match = re.search(r'interface\[name=([^\]]+)\]', event.get("name", ""))
            if name_match:
                interface_name = name_match.group(1)

        ifindex = values.get("ifindex") or values.get("/interface/state/ifindex")
        if interface_name and ifindex is not None:
            self.update(source, int(ifindex), interface_name)

    def lookup(self, router_ip: str, ifindex: int) -> str | None:
        key = f"{router_ip}:{ifindex}"
        with self._lock:
            return self._map.get(key)

    @property
    def size(self) -> int:
        return len(self._map)


# ---------------------------------------------------------------------------
# NetBox Cache (reuse pattern from interface telemetry enrichment)
# ---------------------------------------------------------------------------


class NetBoxCache:
    """
    Maps device_name → device metadata, and (device_name, interface_name) → interface metadata.
    Also maintains an IP → device_name reverse index for resolving sFlow agent IPs.
    Populated from netbox-changes compacted topic.
    """

    def __init__(self):
        self._devices: dict[str, dict] = {}
        self._interfaces: dict[str, dict[str, dict]] = {}
        self._ip_to_device: dict[str, str] = {}  # IP → device_name reverse index
        self._lock = threading.Lock()

    def update_from_event(self, event: dict) -> None:
        device_name = event.get("device_name", "")
        if not device_name:
            return

        with self._lock:
            if event.get("event_type") == "deleted":
                # Remove IP index entries for this device
                old_dev = self._devices.get(device_name, {})
                old_ip = old_dev.get("primary_ip")
                if old_ip:
                    self._ip_to_device.pop(old_ip, None)
                    # Also remove bare IP (without /prefix)
                    self._ip_to_device.pop(old_ip.split("/")[0], None)
                self._devices.pop(device_name, None)
                self._interfaces.pop(device_name, None)
                return

            self._devices[device_name] = {
                "site_name": event.get("site_name"),
                "site_slug": event.get("site_slug"),
                "region": event.get("region"),
                "device_role": event.get("device_role"),
                "device_type": event.get("device_type"),
                "tenant_name": event.get("tenant_name"),
                "primary_ip": event.get("primary_ip"),
                "device_tags": event.get("device_tags", []),
            }

            # Build IP → device_name reverse index
            primary_ip = event.get("primary_ip")
            if primary_ip:
                # Store both with and without CIDR prefix
                self._ip_to_device[primary_ip] = device_name
                bare_ip = primary_ip.split("/")[0]
                self._ip_to_device[bare_ip] = device_name

            # Also index any management IPs from interfaces
            for iface in event.get("interfaces", []):
                for ip_info in iface.get("ip_addresses", []):
                    ip_addr = ip_info if isinstance(ip_info, str) else ip_info.get("address", "")
                    if ip_addr:
                        self._ip_to_device[ip_addr] = device_name
                        self._ip_to_device[ip_addr.split("/")[0]] = device_name

            iface_map = {}
            for iface in event.get("interfaces", []):
                iface_map[iface["name"]] = {
                    "interface_type": iface.get("type"),
                    "interface_enabled": iface.get("enabled"),
                    "cable_peer_device": iface.get("cable_peer_device"),
                    "cable_peer_interface": iface.get("cable_peer_interface"),
                    "cable_status": iface.get("cable_status"),
                    "interface_tags": iface.get("tags", []),
                }
            self._interfaces[device_name] = iface_map

    def get_device_by_ip(self, ip: str) -> tuple[str | None, dict | None]:
        """Resolve an IP to (device_name, device_meta). Returns (None, None) if not found."""
        with self._lock:
            device_name = self._ip_to_device.get(ip)
            if device_name:
                return device_name, self._devices.get(device_name)
            return None, None

    def get_device(self, device_name: str) -> dict | None:
        with self._lock:
            return self._devices.get(device_name)

    def get_interface(self, device_name: str, interface_name: str) -> dict | None:
        with self._lock:
            ifaces = self._interfaces.get(device_name)
            return ifaces.get(interface_name) if ifaces else None

    @property
    def device_count(self) -> int:
        return len(self._devices)

    @property
    def interface_count(self) -> int:
        return sum(len(v) for v in self._interfaces.values())

    @property
    def ip_index_count(self) -> int:
        return len(self._ip_to_device)


# ---------------------------------------------------------------------------
# Cache instances
# ---------------------------------------------------------------------------

_ifindex_cache = IfIndexCache()
_netbox_cache = NetBoxCache()

# ---------------------------------------------------------------------------
# Cache bootstrap threads (assign from offset 0, no consumer group)
# ---------------------------------------------------------------------------


def _bootstrap_ifindex_cache() -> None:
    """Read ifindex-map compacted topic from beginning."""
    log.info("ifindex cache: starting bootstrap")
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"{CONSUMER_GROUP}-ifindex-ephemeral",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)

    meta = consumer.list_topics(IFINDEX_TOPIC, timeout=10)
    topic_meta = meta.topics.get(IFINDEX_TOPIC)
    if not topic_meta or not topic_meta.partitions:
        log.warning("ifindex cache: topic %s not found, will retry", IFINDEX_TOPIC)
        consumer.close()
        return

    partitions = [TopicPartition(IFINDEX_TOPIC, p, OFFSET_BEGINNING) for p in topic_meta.partitions]
    consumer.assign(partitions)

    bootstrap_done = False
    empty_polls = 0
    count = 0

    while not _shutdown.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            empty_polls += 1
            if not bootstrap_done and empty_polls >= 5:
                bootstrap_done = True
                log.info("ifindex cache: bootstrap complete — %d mappings", _ifindex_cache.size)
            continue

        empty_polls = 0
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                if not bootstrap_done:
                    bootstrap_done = True
                    log.info("ifindex cache: bootstrap complete (EOF) — %d mappings", _ifindex_cache.size)
                continue
            log.error("ifindex cache error: %s", msg.error())
            continue

        try:
            event = json.loads(msg.value().decode("utf-8"))
            # gnmic can produce arrays of events
            events = event if isinstance(event, list) else [event]
            for e in events:
                _ifindex_cache.update_from_gnmic_event(e)
                count += 1
                if not bootstrap_done and count % 100 == 0:
                    log.info("ifindex cache: %d events processed", count)
        except Exception as e:
            log.error("ifindex cache: parse error: %s", e)

    consumer.close()
    log.info("ifindex cache: consumer closed")


def _bootstrap_netbox_cache() -> None:
    """Read netbox-changes compacted topic from beginning."""
    log.info("NetBox cache: starting bootstrap")
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"{CONSUMER_GROUP}-netbox-ephemeral",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)

    meta = consumer.list_topics(NETBOX_TOPIC, timeout=10)
    topic_meta = meta.topics.get(NETBOX_TOPIC)
    if not topic_meta or not topic_meta.partitions:
        log.warning("NetBox cache: topic %s not found", NETBOX_TOPIC)
        consumer.close()
        return

    partitions = [TopicPartition(NETBOX_TOPIC, p, OFFSET_BEGINNING) for p in topic_meta.partitions]
    consumer.assign(partitions)

    bootstrap_done = False
    empty_polls = 0

    while not _shutdown.is_set():
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            empty_polls += 1
            if not bootstrap_done and empty_polls >= 5:
                bootstrap_done = True
                log.info("NetBox cache: bootstrap complete — %d devices, %d interfaces",
                         _netbox_cache.device_count, _netbox_cache.interface_count)
            continue

        empty_polls = 0
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                if not bootstrap_done:
                    bootstrap_done = True
                    log.info("NetBox cache: bootstrap complete (EOF) — %d devices",
                             _netbox_cache.device_count)
                continue
            log.error("NetBox cache error: %s", msg.error())
            continue

        try:
            # netbox-changes uses Avro but may also be JSON depending on bridge config
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Might be Avro-encoded — skip for now, needs deserializer
                continue
            _netbox_cache.update_from_event(event)
        except Exception as e:
            log.error("NetBox cache: parse error: %s", e)

    consumer.close()
    log.info("NetBox cache: consumer closed")


# ---------------------------------------------------------------------------
# Community classification
# ---------------------------------------------------------------------------


def classify_communities(communities_str: str) -> list[str]:
    """
    Parse BGP standard communities string and return interconnection types.
    sfacctd outputs communities as space-separated "ASN:VALUE" pairs.
    """
    if not communities_str:
        return []

    types = []
    for comm in communities_str.split():
        comm = comm.strip()
        if comm in COMMUNITY_MAP:
            types.append(COMMUNITY_MAP[comm])
    return types


# ---------------------------------------------------------------------------
# Flow enrichment
# ---------------------------------------------------------------------------


def enrich_flow(raw: dict) -> dict:
    """
    Enrich a raw sfacctd flow record with:
    - Router identity (IP → device_name via NetBox)
    - Interface names (ifindex → name via gNMI-sourced cache)
    - NetBox device/interface metadata (site, role, cable peer, etc.)
    - Interconnection type (BGP communities → classification)
    - AS path context
    """
    enriched = dict(raw)
    enriched["enrichment_ts"] = int(time.time() * 1000)
    enriched["enriched"] = False

    router_ip = raw.get("peer_src_ip", "") or raw.get("agent_id", "")

    # --- Resolve router IP → device identity via NetBox ---
    device_name, device_meta = _netbox_cache.get_device_by_ip(router_ip)
    enriched["device_name"] = device_name
    if device_meta:
        enriched["site_name"] = device_meta.get("site_name")
        enriched["site_slug"] = device_meta.get("site_slug")
        enriched["region"] = device_meta.get("region")
        enriched["device_role"] = device_meta.get("device_role")
        enriched["device_type"] = device_meta.get("device_type")
        enriched["tenant_name"] = device_meta.get("tenant_name")
        enriched["enriched"] = True

    # --- Resolve ifindex → interface name ---
    in_ifindex = raw.get("iface_in", 0) or raw.get("in_iface", 0)
    out_ifindex = raw.get("iface_out", 0) or raw.get("out_iface", 0)

    in_iface_name = _ifindex_cache.lookup(router_ip, in_ifindex) if in_ifindex else None
    out_iface_name = _ifindex_cache.lookup(router_ip, out_ifindex) if out_ifindex else None

    enriched["in_interface_name"] = in_iface_name
    enriched["out_interface_name"] = out_iface_name

    # --- NetBox interface metadata (cable peer, tags, etc.) ---
    if device_name and in_iface_name:
        in_iface_meta = _netbox_cache.get_interface(device_name, in_iface_name)
        if in_iface_meta:
            enriched["in_cable_peer_device"] = in_iface_meta.get("cable_peer_device")
            enriched["in_cable_peer_interface"] = in_iface_meta.get("cable_peer_interface")
            enriched["in_interface_type"] = in_iface_meta.get("interface_type")
            enriched["in_interface_tags"] = in_iface_meta.get("interface_tags", [])

    if device_name and out_iface_name:
        out_iface_meta = _netbox_cache.get_interface(device_name, out_iface_name)
        if out_iface_meta:
            enriched["out_cable_peer_device"] = out_iface_meta.get("cable_peer_device")
            enriched["out_cable_peer_interface"] = out_iface_meta.get("cable_peer_interface")
            enriched["out_interface_type"] = out_iface_meta.get("interface_type")
            enriched["out_interface_tags"] = out_iface_meta.get("interface_tags", [])

    # --- BGP community → interconnection type classification ---
    src_comms = raw.get("comms_src", "") or raw.get("src_std_comm", "")
    dst_comms = raw.get("comms", "") or raw.get("std_comm", "")

    src_types = classify_communities(src_comms)
    dst_types = classify_communities(dst_comms)

    enriched["src_interconnection_types"] = src_types
    enriched["dst_interconnection_types"] = dst_types

    # Primary interconnection type (first match, prefer dst direction)
    if dst_types:
        enriched["interconnection_type"] = dst_types[0]
    elif src_types:
        enriched["interconnection_type"] = src_types[0]
    else:
        enriched["interconnection_type"] = "unknown"

    # --- AS path context ---
    as_path = raw.get("as_path", "")
    src_as_path = raw.get("src_as_path", "")
    if as_path:
        hops = [h for h in as_path.split() if h.isdigit()]
        enriched["as_path_length"] = len(hops)
        enriched["origin_as"] = int(hops[-1]) if hops else None
    else:
        enriched["as_path_length"] = 0
        enriched["origin_as"] = None

    if in_iface_name or out_iface_name or device_name or src_types or dst_types or as_path:
        enriched["enriched"] = True

    return enriched


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main():
    log.info("Starting flow enrichment consumer")
    log.info("Config — bootstrap=%s group=%s", KAFKA_BOOTSTRAP, CONSUMER_GROUP)
    log.info("Topics — raw=%s enriched=%s ifindex=%s netbox=%s",
             RAW_TOPIC, ENRICHED_TOPIC, IFINDEX_TOPIC, NETBOX_TOPIC)

    # Start cache bootstrap threads
    ifindex_thread = threading.Thread(target=_bootstrap_ifindex_cache, daemon=True, name="ifindex-cache")
    netbox_thread = threading.Thread(target=_bootstrap_netbox_cache, daemon=True, name="netbox-cache")
    ifindex_thread.start()
    netbox_thread.start()

    # Wait for caches to get some data
    log.info("Waiting for cache bootstrap...")
    for _ in range(30):
        if _ifindex_cache.size > 0 or _netbox_cache.device_count > 0 or _shutdown.is_set():
            break
        time.sleep(1)
    log.info("Caches ready — ifindex=%d netbox_devices=%d netbox_interfaces=%d ip_index=%d",
             _ifindex_cache.size, _netbox_cache.device_count, _netbox_cache.interface_count,
             _netbox_cache.ip_index_count)

    # Main consumer + producer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 50,
        "batch.num.messages": 500,
        "compression.type": "lz4",
    })

    consumer.subscribe([RAW_TOPIC])
    log.info("Subscribed to %s", RAW_TOPIC)

    msg_count = 0
    enriched_count = 0
    last_log = time.monotonic()

    try:
        while not _shutdown.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                producer.flush(0.1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                raw = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                log.error("Failed to parse flow: %s", e)
                consumer.commit(message=msg, asynchronous=False)
                continue

            enriched = enrich_flow(raw)
            if enriched.get("enriched"):
                enriched_count += 1

            key = f"{enriched.get('peer_src_ip', '')}:{enriched.get('src_host', '')}:{enriched.get('dst_host', '')}"

            producer.produce(
                topic=ENRICHED_TOPIC,
                key=key.encode("utf-8"),
                value=json.dumps(enriched).encode("utf-8"),
            )
            producer.poll(0)
            consumer.commit(message=msg, asynchronous=False)
            msg_count += 1

            now = time.monotonic()
            if now - last_log >= 60:
                enrich_pct = (enriched_count / msg_count * 100) if msg_count else 0
                log.info("Stats — flows=%d enriched=%d (%.1f%%) ifindex=%d devices=%d ips=%d",
                         msg_count, enriched_count, enrich_pct,
                         _ifindex_cache.size, _netbox_cache.device_count,
                         _netbox_cache.ip_index_count)
                last_log = now

    except KafkaException as e:
        log.error("Kafka error: %s", e)
        raise
    finally:
        producer.flush(10)
        consumer.close()
        log.info("Shutdown complete (flows=%d enriched=%d)", msg_count, enriched_count)


if __name__ == "__main__":
    main()
