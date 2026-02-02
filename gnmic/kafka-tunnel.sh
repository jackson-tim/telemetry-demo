#!/bin/bash
# SSH tunnel to access Kafka and Schema Registry from Mac mini
# Kafka ClusterIP: 10.43.236.67:9092 → localhost:9092
# Schema Registry ClusterIP: 10.43.154.3:8081 → localhost:8081
#
# Note: For production, consider:
#   1. Running gnmic inside k8s (preferred)
#   2. Creating a NodePort service (Cilium may need config)
#   3. Using Caddy/ingress for external Kafka access

set -e

echo "Setting up SSH tunnel to k8s Kafka cluster..."
echo "  Kafka: localhost:9092 → kafka.telemetry.svc.cluster.local:9092"
echo "  Schema Registry: localhost:8081 → schema-registry.telemetry.svc.cluster.local:8081"
echo ""
echo "Press Ctrl+C to stop the tunnel."

ssh -L 9092:10.43.236.67:9092 \
    -L 8081:10.43.154.3:8081 \
    k8s-lb -N
