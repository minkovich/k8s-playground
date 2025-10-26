# Custom Kubernetes Scheduler

A production-ready custom Kubernetes scheduler implementing priority-based scheduling, preemption, and gang-scheduling for pod groups.

## Features

### Core Features
- **Priority-Based Scheduling**: Schedules pods based on priority annotations, with higher priority pods getting scheduled first
- **One Pod Per Node**: Enforces a constraint of one pod per node (simplified resource allocation)
- **Preemption**: Automatically evicts lower priority pods to make room for higher priority ones
- **Gang-Scheduling**: Ensures all pods in a group are scheduled together or none are scheduled (all-or-nothing semantics)
- **Real-time Cluster State Tracking**: Maintains accurate view of node assignments and pod groups

### Implementation Highlights
- Written in Python using the official Kubernetes client library
- Watches pod events in real-time using the Kubernetes watch API
- Properly handles pod lifecycle events (ADDED, DELETED)
- Implements atomic gang-scheduling decisions
- Comprehensive error handling and logging

## Architecture

### High-Level Design

The scheduler is split into two clean layers:

```
┌─────────────────────────────────────────────────────┐
│                 Kubernetes API Server                │
└────────────────────┬────────────────────────────────┘
                     │
                     │ Watch Pod Events / Bindings
                     │
┌────────────────────▼────────────────────────────────┐
│         scheduler.py (K8s Adapter Layer)             │
│  - Watches pod events from K8s API                   │
│  - Converts K8s objects to simple dicts              │
│  - Executes bind/preempt actions via K8s API         │
│  - Handles API errors and retries                    │
└────────────────────┬────────────────────────────────┘
                     │
                     │ Simple dicts (no K8s types)
                     │
┌────────────────────▼────────────────────────────────┐
│    scheduling_logic.py (Pure Scheduling Logic)       │
│  - No K8s dependencies (pure Python)                 │
│  - Maintains cluster state (nodes, pods, priorities) │
│  - Creates deterministic scheduling plan             │
│  - Returns actions: {"bind": [...], "preempt": [...]}│
│                                                       │
│  Key Components:                                     │
│  • Node assignments map (one pod per node)           │
│  • Scheduling queue (priority-ordered units)         │
│  • Gang grouping logic                               │
│  • Plan creation (deterministic)                     │
│  • Plan-to-actions reconciliation (stable)           │
└──────────────────────────────────────────────────────┘
```

**Benefits of this architecture:**
- Scheduling logic is testable without K8s cluster
- Clear separation of concerns
- Easy to reason about and maintain
- K8s-agnostic logic can be reused in other contexts

### Scheduling Algorithm

The scheduler uses a **deterministic, plan-based approach**:

#### Planning Phase (Deterministic)
1. **Rebuild scheduling queue**: Collect all unscheduled pods into scheduling units
   - Single pods → individual units
   - Gang pods (same `pod-group`) → grouped into one unit
   - Each unit has an effective priority (min priority for gangs)
   
2. **Create ideal plan**: Determine which units *should* be scheduled
   - Sort units by priority (higher first)
   - When priorities are equal, smaller units (fewer pods) go first
   - Select top units that fit within total cluster capacity
   - This is deterministic: same inputs → same plan

#### Reconciliation Phase (Stable)
3. **Generate minimal actions**: Compare plan to current state
   - **Preserve existing assignments**: Pods already on correct nodes stay put
   - **Identify victims**: Pods not in plan must be preempted
   - **Assign new pods**: Bind unscheduled pods from plan to available nodes
   - **Minimize churn**: Only move pods when necessary

4. **Execute actions**: Apply bind/preempt operations via K8s API
   - Preempt lower-priority pods first
   - Bind new pods to freed or available nodes
   - Handle API errors with retry/recovery

#### Gang-Scheduling Guarantees
- **All-or-nothing**: Either entire gang schedules or none of it does
- **Atomic decisions**: Gang evaluated as single unit in planning
- **Priority-based**: Gang priority = minimum priority of its members
- **Stable**: Once scheduled, gang members stay unless higher priority work arrives

### Priority System

Pods can specify priority in two ways:
1. **Annotation** (preferred): `priority: "100"` in pod metadata annotations
2. **PriorityClass**: Using Kubernetes native `spec.priority`

Higher numbers indicate higher priority. Default priority is 0.

### Gang-Scheduling

Pods belonging to the same gang specify a pod group annotation:
```yaml
metadata:
  annotations:
    pod-group: "my-group-name"
```

All pods with the same `pod-group` value will be scheduled together atomically.

## Setup

### Prerequisites
- Kubernetes cluster (can be local with minikube)
- kubectl configured to access the cluster
- Docker or Podman for building the scheduler image

### Quick Start with Minikube

1. **Create a multi-node cluster**:
```bash
minikube start --nodes 3 -p custom-scheduler-demo
```

2. **Run the setup script**:
```bash
./setup.sh
```

This will:
- Build the scheduler Docker image
- Load the image into minikube
- Create RBAC resources (ServiceAccount, ClusterRole, ClusterRoleBinding)
- Deploy the scheduler as a Deployment in kube-system namespace
- Wait for the scheduler to be ready

3. **Verify the scheduler is running**:
```bash
kubectl get pods -n kube-system -l app=custom-scheduler
kubectl logs -n kube-system -l app=custom-scheduler
```

### Custom Installation

To install in a different namespace:
```bash
./setup.sh my-namespace
```

**Note:** The setup script builds `custom-scheduler:latest` by default. If you want to use a different tag or registry, you need to:
```bash
./setup.sh kube-system custom-scheduler:v1.0 custom-scheduler-demo
```

## Usage

### Deploying Pods with Custom Scheduler

To use the custom scheduler, pods must specify `schedulerName: custom-scheduler` in their spec.

### Testing with Example Deployments

Deploy the included example deployments:
```bash
kubectl apply -f k8s/example-deployments.yaml
```

This creates three deployments that demonstrate all scheduler features:
- **High-priority deployment** (priority: 100) - single replica
- **Low-priority deployment** (priority: 10) - single replica  
- **Gang-scheduled deployment** "group-a" (priority: 50) - 3 replicas that must schedule together

The example file (`k8s/example-deployments.yaml`) shows how to:
- Set priority using annotations: `priority: "100"`
- Enable gang-scheduling with `pod-group: "group-a"` annotation
- Configure the custom scheduler with `schedulerName: custom-scheduler`

Since these are Deployments, pods will automatically be recreated after preemption, making it easy to observe the scheduler's behavior

Watch the scheduling decisions:
```bash
kubectl logs -n kube-system -l app=custom-scheduler -f
```

Check pod placement:
```bash
kubectl get pods -o wide
```

## Testing

### Running Unit Tests

Install test dependencies:
```bash
cd tests
pip install -r requirements.txt
```

Run tests:
```bash
# Run all tests
python -m pytest test_scheduler.py -v

# Run with coverage
python -m pytest test_scheduler.py -v --cov=../scheduler --cov-report=html
```

### Test Coverage

The test suite includes:
- Cluster state initialization
- Pod priority extraction
- Node availability checking
- Preemption logic
- Single pod scheduling (with and without preemption)
- Gang-scheduling (with sufficient capacity)
- Gang-scheduling (with preemption)
- Gang-scheduling (insufficient capacity - should fail)
- Pod deletion handling
- Priority ordering verification
- Integration scenarios

### Manual Testing Scenarios

#### Scenario 1: Priority Preemption

Test preemption by deploying the example deployments on a cluster with limited capacity:

```bash
# Deploy examples (includes high and low priority pods)
kubectl apply -f k8s/example-deployments.yaml

# Watch as the high-priority pod preempts lower priority pods if needed
kubectl get pods -o wide --watch
```

Observe in the logs how the scheduler preempts lower-priority pods to make room for higher-priority ones.

#### Scenario 2: Gang-Scheduling

The included `k8s/example-deployments.yaml` contains a gang-scheduled deployment with 3 replicas in "group-a". 

```bash
# Deploy the gang
kubectl apply -f k8s/example-deployments.yaml

# Watch the gang members - they should all schedule together or none at all
kubectl get pods -l app=gang-app-a -o wide --watch
```

For more advanced gang-scheduling tests, see `k8s/example-deployments-split-gang.yaml` which has multiple single-replica deployments in the same gang.

#### Scenario 3: Custom Priority Testing

Create your own test pods by following the patterns in `k8s/example-deployments.yaml`. Key requirements:
- Set `schedulerName: custom-scheduler` in the pod spec
- Use `priority: "N"` annotation where N is an integer (higher = more important)
- Use `pod-group: "group-name"` annotation to enable gang-scheduling

## Retry Mechanism (Extra Points)

### Current Implementation

The scheduler implements robust error recovery:

**Automatic State Re-initialization:**
- Prioritize simplicity over efficiency
- On API errors (bind/preempt failures, exceptions), the scheduler re-initializes its internal state
- Creates a fresh `SchedulingLogic` instance and re-reads all pods/nodes from K8s
- 10 second delay before re-init to let cluster state settle
- 30 second cooldown between re-initializations to prevent thrashing

**Smart Error Handling:**
- **404 (Not Found)**: Pod was deleted - notifies logic layer to clean up state without full re-init
- **409 (Conflict)**: Pod already bound - verifies it's on the correct node and treats as success (idempotent bind)

This approach trades some efficiency for correctness - the scheduler always recovers to a known-good state rather than attempting partial fixes.

## Performance Optimization (Extra Points)

### Optimization for Large Scale

The current scheduler uses a **plan-based approach**: on every pod event, it rebuilds the entire scheduling queue and creates a complete scheduling plan. This ensures deterministic, correct decisions but requires O(P) work per event where P = number of pods.

**Recommended optimization for 10,000+ pods:**
- **Incremental planning**: Instead of replanning everything, make incremental scheduling decisions
- Only reconsider scheduling units affected by the event (e.g., pods in the same gang, or only if priority changes)
- Maintain a priority queue of unscheduled pods rather than rebuilding from scratch
- Trade-off: More complex state management, but O(log P) work per event instead of O(P)


## Project Structure

```
k8s-playground/
├── README.md                           # This file
├── setup.sh                            # Deployment script
├── scheduler/
│   ├── scheduler.py                   # K8s adapter layer
│   ├── scheduling_logic.py            # Pure scheduling logic (K8s-agnostic)
│   ├── requirements.txt               # Python dependencies
│   └── Dockerfile                     # Container image definition
├── k8s/
│   ├── rbac.yaml                      # RBAC configuration
│   ├── deployment.yaml                # Scheduler deployment
│   ├── example-deployments.yaml       # Example workloads for testing
│   └── example-deployments-split-gang.yaml  # Advanced gang-scheduling examples
└── tests/
    ├── test_scheduler.py              # Unit tests
    └── requirements.txt               # Test dependencies
```

