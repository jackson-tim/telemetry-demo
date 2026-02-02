#!/usr/bin/env python3
"""
NetBox → Kafka CDC Bridge
=========================
Receives NetBox webhooks and produces normalized device-state events
to a compacted Kafka topic (netbox-changes).

Also provides a /sync endpoint to trigger a full bulk export of all
devices from NetBox → topic (bootstrap / schema change re-emit).

Endpoints:
    POST /webhook          — NetBox webhook receiver
    POST /sync             — Full bulk sync from NetBox API
    GET  /sync/status      — Status of last sync
    GET  /health           — Liveness check

Env:
    KAFKA_BOOTSTRAP       — default: kafka:9092
    SCHEMA_REGISTRY_URL   — default: http://schema-registry:8081
    NETBOX_URL            — default: https://netbox.idge.net
    NETBOX_TOKEN          — required
    KAFKA_TOPIC           — default: netbox-changes
    WEBHOOK_SECRET        — optional, validates X-Hook-Secret header
    LOG_LEVEL             — default: INFO
    PORT                  — default: 8090
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import threading
from pathlib import Path
from typing import Any

import pynetbox
import requests.exceptions
from flask import Flask, request, jsonify
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
NETBOX_URL = os.environ.get("NETBOX_URL", "https://netbox.idge.net")
NETBOX_TOKEN = os.environ.get("NETBOX_TOKEN", "")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "netbox-changes")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
PORT = int(os.environ.get("PORT", "8090"))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
log = logging.getLogger("netbox-bridge")

# ---------------------------------------------------------------------------
# Kafka + Avro setup
# ---------------------------------------------------------------------------

SCHEMA_DIR = Path(__file__).parent
SCHEMA_STR = (SCHEMA_DIR / "netbox_device_state.avsc").read_text()

sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
avro_ser = AvroSerializer(
    schema_registry_client=sr_client,
    schema_str=SCHEMA_STR,
    conf={"auto.register.schemas": True},
)
key_ser = StringSerializer("utf_8")

kafka_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "netbox-bridge",
    "linger.ms": 50,
    "batch.num.messages": 100,
    "compression.type": "lz4",
    "acks": "all",
    "enable.idempotence": True,
}
producer = Producer(kafka_conf)

# ---------------------------------------------------------------------------
# NetBox client
# ---------------------------------------------------------------------------


def _get_netbox() -> pynetbox.api:
    nb = pynetbox.api(NETBOX_URL, token=NETBOX_TOKEN)
    nb.http_session.verify = os.environ.get("NETBOX_TLS_VERIFY", "true").lower() in ("true", "1", "yes")
    return nb


# ---------------------------------------------------------------------------
# Device state builder
# ---------------------------------------------------------------------------


def build_device_state(
    device: Any,
    interfaces: list[Any] | None = None,
    event_type: str = "snapshot",
    nb: pynetbox.api | None = None,
) -> dict[str, Any]:
    """Build a NetBoxDeviceState dict from a pynetbox device object."""

    # Resolve region from site
    region = None
    if hasattr(device, "site") and device.site and nb:
        try:
            site_detail = nb.dcim.sites.get(device.site.id)
            if site_detail and site_detail.region:
                region = getattr(site_detail.region, "name", None)
        except Exception:
            pass

    state: dict[str, Any] = {
        "device_name": device.name,
        "event_type": event_type,
        "event_timestamp": int(time.time() * 1000),
        "netbox_id": device.id,
        "site_name": getattr(device.site, "name", None) if device.site else None,
        "site_slug": getattr(device.site, "slug", None) if device.site else None,
        "region": region,
        "rack_name": getattr(device.rack, "name", None) if device.rack else None,
        "rack_position": int(device.position) if device.position is not None else None,
        "rack_face": str(device.face) if device.face else None,
        "device_role": getattr(device.role, "name", None) if device.role else None,
        "device_type": getattr(device.device_type, "display", None) if device.device_type else None,
        "device_platform": getattr(device.platform, "name", None) if device.platform else None,
        "device_serial": device.serial or None,
        "device_status": str(device.status) if device.status else None,
        "device_tags": [t.name for t in (device.tags or [])],
        "primary_ip": str(device.primary_ip) if device.primary_ip else None,
        "tenant_name": getattr(device.tenant, "name", None) if device.tenant else None,
        "tenant_slug": getattr(device.tenant, "slug", None) if device.tenant else None,
        "custom_fields": json.dumps(device.custom_fields) if device.custom_fields else None,
        "interfaces": [],
    }

    # Build interface list
    if interfaces is None and nb:
        try:
            interfaces = list(nb.dcim.interfaces.filter(device_id=device.id))
        except Exception as e:
            log.warning("Failed to fetch interfaces for %s: %s", device.name, e)
            interfaces = []

    for iface in (interfaces or []):
        iface_state: dict[str, Any] = {
            "name": iface.name,
            "netbox_id": iface.id,
            "type": str(iface.type) if iface.type else None,
            "enabled": iface.enabled,
            "mtu": iface.mtu,
            "mac_address": str(iface.mac_address) if iface.mac_address else None,
            "description": iface.description or None,
            "mode": str(iface.mode) if iface.mode else None,
            "tags": [t.name for t in (iface.tags or [])],
            "cable_peer_device": None,
            "cable_peer_interface": None,
            "cable_status": None,
            "cable_type": None,
        }

        # Resolve cable peer
        if iface.cable:
            try:
                cable = nb.dcim.cables.get(iface.cable.id)
                if cable:
                    iface_state["cable_status"] = str(cable.status) if cable.status else None
                    iface_state["cable_type"] = str(cable.type) if cable.type else None
            except Exception:
                pass

        if hasattr(iface, "connected_endpoints") and iface.connected_endpoints:
            try:
                peer = iface.connected_endpoints[0]
                iface_state["cable_peer_device"] = getattr(peer.device, "name", None) if hasattr(peer, "device") else None
                iface_state["cable_peer_interface"] = getattr(peer, "name", None)
            except (IndexError, AttributeError):
                pass

        if not iface_state["cable_peer_device"] and hasattr(iface, "link_peers") and iface.link_peers:
            try:
                peer = iface.link_peers[0]
                iface_state["cable_peer_device"] = getattr(peer.device, "name", None) if hasattr(peer, "device") else None
                iface_state["cable_peer_interface"] = getattr(peer, "name", None)
            except (IndexError, AttributeError):
                pass

        state["interfaces"].append(iface_state)

    return state


def produce_device_state(state: dict[str, Any]) -> None:
    """Serialize and produce a device state event to Kafka."""
    key = state["device_name"]
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=key_ser(key),
            value=avro_ser(state, SerializationContext(KAFKA_TOPIC, MessageField.VALUE)),
            on_delivery=lambda err, msg: (
                log.error("Delivery failed for %s: %s", msg.key(), err) if err
                else log.debug("Delivered %s [%d]@%d", msg.topic(), msg.partition(), msg.offset())
            ),
        )
        producer.poll(0)
    except Exception as e:
        log.error("Failed to produce device state for %s: %s", key, e)
        raise


# ---------------------------------------------------------------------------
# Bulk sync
# ---------------------------------------------------------------------------

_sync_status = {"running": False, "last_run": None, "last_count": 0, "last_error": None}
_sync_lock = threading.Lock()


def _run_bulk_sync() -> None:
    """Fetch all devices from NetBox and produce full snapshots."""
    global _sync_status
    with _sync_lock:
        if _sync_status["running"]:
            log.warning("Sync already in progress, skipping")
            return
        _sync_status["running"] = True

    try:
        log.info("Starting full NetBox → Kafka sync")
        nb = _get_netbox()
        devices = list(nb.dcim.devices.all())
        log.info("Found %d devices to sync", len(devices))

        count = 0
        for device in devices:
            try:
                state = build_device_state(device, event_type="snapshot", nb=nb)
                produce_device_state(state)
                count += 1
                if count % 10 == 0:
                    log.info("Synced %d/%d devices", count, len(devices))
                    producer.flush(timeout=5.0)
            except Exception as e:
                log.error("Failed to sync device %s: %s", device.name, e)

        producer.flush(timeout=30.0)
        _sync_status.update(last_run=time.time(), last_count=count, last_error=None)
        log.info("Bulk sync complete: %d devices", count)

    except Exception as e:
        log.error("Bulk sync failed: %s", e)
        _sync_status["last_error"] = str(e)
    finally:
        _sync_status["running"] = False


# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/sync/status", methods=["GET"])
def sync_status():
    return jsonify(_sync_status), 200


@app.route("/sync", methods=["POST"])
def trigger_sync():
    """Trigger a full bulk sync in a background thread."""
    if _sync_status["running"]:
        return jsonify({"error": "sync already in progress"}), 409
    t = threading.Thread(target=_run_bulk_sync, daemon=True)
    t.start()
    return jsonify({"status": "sync started"}), 202


@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Receive a NetBox webhook.
    NetBox sends: {event, timestamp, model, username, request_id, data, snapshots}
    We care about dcim.device and dcim.interface events.
    """
    # Validate secret if configured
    if WEBHOOK_SECRET:
        header_secret = request.headers.get("X-Hook-Secret", "")
        if header_secret != WEBHOOK_SECRET:
            log.warning("Webhook secret mismatch")
            return jsonify({"error": "invalid secret"}), 403

    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "no JSON body"}), 400

    event = payload.get("event", "")
    model = payload.get("model", "")
    data = payload.get("data", {})

    log.info("Webhook received: event=%s model=%s id=%s", event, model, data.get("id"))

    # We handle device and interface events
    if model not in ("dcim.device", "dcim.interface"):
        log.debug("Ignoring model %s", model)
        return jsonify({"status": "ignored", "reason": f"model {model} not tracked"}), 200

    try:
        nb = _get_netbox()

        if model == "dcim.device":
            device_id = data.get("id")
            if "deleted" in event:
                # Produce a tombstone-like record
                device_name = data.get("name", data.get("display", f"unknown-{device_id}"))
                state = {
                    "device_name": device_name,
                    "event_type": "deleted",
                    "event_timestamp": int(time.time() * 1000),
                    "netbox_id": device_id,
                    "site_name": None, "site_slug": None, "region": None,
                    "rack_name": None, "rack_position": None, "rack_face": None,
                    "device_role": None, "device_type": None, "device_platform": None,
                    "device_serial": None, "device_status": None, "device_tags": [],
                    "primary_ip": None, "tenant_name": None, "tenant_slug": None,
                    "custom_fields": None, "interfaces": [],
                }
                produce_device_state(state)
                producer.flush(timeout=5.0)
                return jsonify({"status": "produced", "device": device_name, "event": "deleted"}), 200
            else:
                # Fetch fresh from API (webhook data may be stale)
                device = nb.dcim.devices.get(device_id)
                if not device:
                    return jsonify({"error": f"device {device_id} not found"}), 404
                event_type = "created" if "created" in event else "updated"
                state = build_device_state(device, event_type=event_type, nb=nb)
                produce_device_state(state)
                producer.flush(timeout=5.0)
                return jsonify({"status": "produced", "device": device.name, "event": event_type}), 200

        elif model == "dcim.interface":
            # Interface changed — re-emit the parent device's full state
            device_data = data.get("device")
            if not device_data:
                return jsonify({"error": "no device in interface payload"}), 400

            device_id = device_data.get("id") if isinstance(device_data, dict) else device_data
            device = nb.dcim.devices.get(device_id)
            if not device:
                return jsonify({"error": f"device {device_id} not found"}), 404

            state = build_device_state(device, event_type="updated", nb=nb)
            produce_device_state(state)
            producer.flush(timeout=5.0)
            return jsonify({"status": "produced", "device": device.name, "event": "updated", "trigger": "interface_change"}), 200

    except Exception as e:
        log.error("Webhook processing failed: %s", e)
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def on_startup():
    """Run initial sync on startup if topic is empty."""
    log.info("NetBox CDC Bridge starting")
    log.info("Kafka: %s | Schema Registry: %s | NetBox: %s", KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL, NETBOX_URL)
    log.info("Topic: %s", KAFKA_TOPIC)

    # Auto-sync on first boot
    if os.environ.get("AUTO_SYNC_ON_START", "true").lower() in ("true", "1", "yes"):
        log.info("Auto-sync enabled — triggering initial bulk sync")
        t = threading.Thread(target=_run_bulk_sync, daemon=True)
        t.start()


on_startup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
