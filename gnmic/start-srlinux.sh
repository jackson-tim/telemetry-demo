#!/bin/bash
# Start Nokia SR Linux container as a gNMI test target
# Exposes gNMI on port 57400, SSH on 22022, JSON-RPC on 443
# Default credentials: admin / NokiaSrl1!

set -e

CONTAINER_NAME="srlinux-test"
IMAGE="ghcr.io/nokia/srlinux:latest"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Check if already running
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "SR Linux container '${CONTAINER_NAME}' is already running."
    echo "gNMI endpoint: localhost:57400"
    echo "  Username: admin"
    echo "  Password: NokiaSrl1!"
    exit 0
fi

# Remove old stopped container if exists
docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true

# Ensure topology file exists
if [ ! -f "${SCRIPT_DIR}/topology.yml" ]; then
    cat > "${SCRIPT_DIR}/topology.yml" << 'EOF'
chassis_configuration:
  "chassis_type": 72
  "base_mac": "1a:b0:00:00:00:00"
  "cpm_card_type": 187

slot_configuration:
  1:
    "card_type": 187
    "mda_type": 200
EOF
fi

echo "Starting SR Linux container..."
docker run -t -d --rm --privileged \
    -v "${SCRIPT_DIR}/topology.yml":/tmp/topology.yml \
    -p 57400:57400 \
    -p 22022:22 \
    -p 8443:443 \
    --name "${CONTAINER_NAME}" \
    "${IMAGE}" \
    sudo bash /opt/srlinux/bin/sr_linux

echo "Waiting for SR Linux to boot (this takes ~45-60 seconds)..."
for i in $(seq 1 60); do
    if gnmic -a localhost:57400 --skip-verify -u admin -p 'NokiaSrl1!' capabilities 2>/dev/null | grep -q "gNMI version"; then
        echo ""
        echo "✅ SR Linux is ready!"
        echo ""
        echo "gNMI endpoint: localhost:57400"
        echo "  Username: admin"
        echo "  Password: NokiaSrl1!"
        echo "  TLS: self-signed (use --skip-verify)"
        echo ""
        echo "Quick test commands:"
        echo "  gnmic -a localhost:57400 --skip-verify -u admin -p 'NokiaSrl1!' capabilities"
        echo "  gnmic -a localhost:57400 --skip-verify -u admin -p 'NokiaSrl1!' get --path '/interface[name=mgmt0]'"
        echo "  gnmic -a localhost:57400 --skip-verify -u admin -p 'NokiaSrl1!' subscribe --path '/interface[name=mgmt0]/statistics' --stream-mode sample --sample-interval 5s --format event"
        exit 0
    fi
    printf "."
    sleep 2
done

echo ""
echo "⚠️  SR Linux may still be booting. Check: docker logs ${CONTAINER_NAME}"
