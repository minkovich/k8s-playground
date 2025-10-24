#!/bin/bash
set -e
# set -x # Uncomment this for debugging

# Custom Kubernetes Scheduler Setup Script
# This script deploys the custom scheduler to an existing Kubernetes cluster

NAMESPACE="${1:-kube-system}"
SCHEDULER_IMAGE="${2:-custom-scheduler:latest}"
CLUSTER_NAME="${3:-custom-scheduler-demo}"

echo "===================================="
echo "Custom Scheduler Setup"
echo "===================================="
echo "Namespace: $NAMESPACE"
echo "Scheduler Image: $SCHEDULER_IMAGE"
echo ""

# Check if kubectl is available
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl is not installed or not in PATH"
    exit 1
fi

# Check if cluster is accessible
if ! kubectl cluster-info &> /dev/null; then
    echo "Error: Cannot connect to Kubernetes cluster"
    echo "Please ensure your kubeconfig is properly configured"
    exit 1
fi

echo "✓ kubectl is available"
echo "✓ Connected to cluster: $(kubectl config current-context)"
echo ""

# Build the scheduler Docker image
echo "Building scheduler Docker image..."
cd "$(dirname "$0")"

if command -v docker &> /dev/null; then
    docker build -t "$SCHEDULER_IMAGE" ./scheduler
    echo "✓ Docker image built: $SCHEDULER_IMAGE"
    
    # If using minikube, load the image
    if kubectl config current-context | grep -q "custom-scheduler-demo"; then
        echo "Detected minikube, loading image into minikube..."
        minikube image load "$SCHEDULER_IMAGE" -p "$CLUSTER_NAME" || echo "Warning: Failed to load image into minikube"
    fi
elif command -v podman &> /dev/null; then
    podman build -t "$SCHEDULER_IMAGE" ./scheduler
    echo "✓ Podman image built: $SCHEDULER_IMAGE"
    
    # If using minikube with podman
    if kubectl config current-context | grep -q "minikube"; then
        echo "Detected minikube, loading image into minikube..."
        minikube image load "$SCHEDULER_IMAGE" || echo "Warning: Failed to load image into minikube"
    fi
else
    echo "Warning: Neither docker nor podman found. Assuming image is already available."
fi

echo ""

# Create namespace if it doesn't exist (only if not using kube-system)
if [ "$NAMESPACE" != "kube-system" ]; then
    echo "Creating namespace $NAMESPACE if it doesn't exist..."
    kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    echo "✓ Namespace ready"
    echo ""
fi

# Update deployment manifest with custom namespace if needed
DEPLOYMENT_YAML=$(mktemp)
RBAC_YAML=$(mktemp)

sed "s/namespace: kube-system/namespace: $NAMESPACE/g" k8s/deployment.yaml > "$DEPLOYMENT_YAML"
sed "s/namespace: kube-system/namespace: $NAMESPACE/g" k8s/rbac.yaml > "$RBAC_YAML"

# Update image in deployment
sed -i.bak "s|image: custom-scheduler:latest|image: $SCHEDULER_IMAGE|g" "$DEPLOYMENT_YAML"
rm -f "${DEPLOYMENT_YAML}.bak"

# Apply RBAC configuration
echo "Applying RBAC configuration..."
kubectl apply -f "$RBAC_YAML"
echo "✓ RBAC configured"
echo ""

# Deploy the scheduler
echo "Deploying custom scheduler..."
kubectl apply -f "$DEPLOYMENT_YAML"
echo "✓ Scheduler deployed"
echo ""

# Cleanup temp files
rm -f "$DEPLOYMENT_YAML" "$RBAC_YAML"

# Wait for scheduler to be ready
echo "Waiting for scheduler to be ready..."
kubectl wait --for=condition=available --timeout=60s \
    deployment/custom-scheduler -n "$NAMESPACE" || {
    echo "Warning: Timeout waiting for scheduler. Checking status..."
    kubectl get pods -n "$NAMESPACE" -l app=custom-scheduler
}

echo ""
echo "===================================="
echo "Setup Complete!"
echo "===================================="
echo ""
echo "Scheduler Status:"
kubectl get pods -n "$NAMESPACE" -l app=custom-scheduler
echo ""
echo "To view scheduler logs:"
echo "  kubectl logs -n $NAMESPACE -l app=custom-scheduler -f"
echo ""
echo "To deploy test workloads:"
echo "  kubectl apply -f k8s/example-deployments.yaml"
echo ""
echo "To see preemption with automatic pod recreation:"
echo "  ./demo-preemption.sh"
echo ""
echo "To uninstall:"
echo "  kubectl delete -f k8s/deployment.yaml"
echo "  kubectl delete -f k8s/rbac.yaml"
echo ""

