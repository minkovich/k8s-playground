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
- Properly handles pod lifecycle events (ADDED, DELETED, MODIFIED)
- Implements atomic gang-scheduling decisions
- Comprehensive error handling and logging

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────┐
│                 Kubernetes API Server                │
└────────────────────┬────────────────────────────────┘
                     │
                     │ Watch Pod Events
                     │
┌────────────────────▼────────────────────────────────┐
│            Custom Scheduler (Python)                 │
│                                                      │
│  ┌────────────────────────────────────────────┐    │
│  │  Event Loop (Pod Watch)                    │    │
│  │   - ADDED: Schedule new pods               │    │
│  │   - DELETED: Update node tracking          │    │
│  └────────────────┬───────────────────────────┘    │
│                   │                                  │
│  ┌────────────────▼───────────────────────────┐    │
│  │  Scheduling Logic                          │    │
│  │   - Check if pod is gang-scheduled         │    │
│  │   - Find available nodes                   │    │
│  │   - Try preemption if needed               │    │
│  │   - Bind pod(s) to node(s)                 │    │
│  └────────────────┬───────────────────────────┘    │
│                   │                                  │
│  ┌────────────────▼───────────────────────────┐    │
│  │  State Management                          │    │
│  │   - Node assignments map                   │    │
│  │   - Pod group tracking                     │    │
│  │   - Pending gang-scheduled pods            │    │
│  └────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

### Scheduling Algorithm

#### Single Pod Scheduling
1. Check for available nodes (nodes without assigned pods)
2. If available node exists, bind pod to node
3. If no available nodes, attempt preemption:
   - Find nodes with lower priority pods
   - Sort by priority (lowest first)
   - Preempt lowest priority pod
   - Bind new pod to freed node

#### Gang-Scheduling Algorithm
1. Collect all pods in the same pod group
2. Determine required node count (number of pods in group)
3. Find available nodes
4. If insufficient nodes, find preemption candidates:
   - Only preempt if all victims have lower priority than gang's max priority
   - Preempt lowest priority pods first
5. If sufficient capacity (available + preemptable), schedule all pods atomically
6. If insufficient capacity, fail the entire gang (schedule nothing)

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

**Note:** The setup script builds `custom-scheduler:latest` by default. If you want to use a different tag or registry, you need to build and push it first:
```bash
# Build and tag your image
docker build -t my-registry/custom-scheduler:v1.0 ./scheduler
docker push my-registry/custom-scheduler:v1.0

# Then deploy
./setup.sh kube-system my-registry/custom-scheduler:v1.0
```

## Usage

### Deploying Pods with Custom Scheduler

To use the custom scheduler, pods must specify `schedulerName: custom-scheduler`:

#### Example: High Priority Pod
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: high-priority-pod
  annotations:
    priority: "100"
spec:
  schedulerName: custom-scheduler
  containers:
  - name: nginx
    image: nginx:alpine
```

#### Example: Gang-Scheduled Pods
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: worker-1
  annotations:
    priority: "50"
    pod-group: "distributed-job"
spec:
  schedulerName: custom-scheduler
  containers:
  - name: worker
    image: my-worker:latest
---
apiVersion: v1
kind: Pod
metadata:
  name: worker-2
  annotations:
    priority: "50"
    pod-group: "distributed-job"
spec:
  schedulerName: custom-scheduler
  containers:
  - name: worker
    image: my-worker:latest
```

### Testing with Example Pods

Deploy the included example deployments:
```bash
kubectl apply -f k8s/example-deployments.yaml
```

This creates deployments that will automatically recreate pods after preemption:
- One high-priority pod (priority: 100)
- One low-priority pod (priority: 10)
- Three gang-scheduled pods in "group-a" (priority: 50)

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
```bash
# Fill cluster with low priority pods
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: low-1
  annotations:
    priority: "10"
spec:
  schedulerName: custom-scheduler
  containers:
  - name: nginx
    image: nginx:alpine
EOF

# Deploy high priority pod - should preempt low priority pod
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: high-1
  annotations:
    priority: "100"
spec:
  schedulerName: custom-scheduler
  containers:
  - name: nginx
    image: nginx:alpine
EOF
```

#### Scenario 2: Gang-Scheduling Success
```bash
# Deploy gang with enough capacity
for i in {1..3}; do
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: gang-success-$i
  annotations:
    priority: "50"
    pod-group: "success-group"
spec:
  schedulerName: custom-scheduler
  containers:
  - name: nginx
    image: nginx:alpine
EOF
done
```

#### Scenario 3: Gang-Scheduling Failure
```bash
# First, fill the cluster completely with high priority pods
# Then try to deploy a gang that requires more nodes than available
# The gang should fail to schedule
```

## Retry Mechanism (Extra Points)

### Current Implementation
The current scheduler operates in a continuous watch loop, processing events as they arrive. Failed scheduling attempts are not automatically retried within the scheduler.

### Recommended Retry Mechanism

For a production-ready retry mechanism, I would implement:

#### 1. Exponential Backoff Retry Queue
```python
class RetryQueue:
    def __init__(self):
        self.pending_pods = {}  # pod_uid -> (pod_info, retry_count, next_retry_time)
        self.max_retries = 5
        self.base_delay = 1.0  # seconds
    
    def add(self, pod_info):
        self.pending_pods[pod_info.uid] = (pod_info, 0, time.time())
    
    def get_ready_pods(self):
        """Return pods ready for retry."""
        now = time.time()
        ready = []
        for uid, (pod_info, count, next_time) in self.pending_pods.items():
            if now >= next_time and count < self.max_retries:
                ready.append((uid, pod_info))
        return ready
    
    def mark_failed(self, pod_uid):
        """Mark a scheduling attempt as failed and schedule retry."""
        if pod_uid in self.pending_pods:
            pod_info, count, _ = self.pending_pods[pod_uid]
            delay = self.base_delay * (2 ** count)  # Exponential backoff
            next_time = time.time() + delay + random.uniform(0, delay * 0.1)  # Add jitter
            self.pending_pods[pod_uid] = (pod_info, count + 1, next_time)
```

#### 2. Event-Driven Retry Triggers
Retries should be triggered not just by time, but also by cluster state changes:
- When a pod is deleted (capacity freed)
- When a new node is added
- When resource pressure decreases

#### 3. Priority-Based Retry Ordering
Higher priority pods should be retried before lower priority ones:
```python
def process_retries(self):
    ready_pods = self.retry_queue.get_ready_pods()
    # Sort by priority descending
    ready_pods.sort(key=lambda x: x[1].priority, reverse=True)
    for uid, pod_info in ready_pods:
        success = self.attempt_schedule(pod_info)
        if not success:
            self.retry_queue.mark_failed(uid)
        else:
            self.retry_queue.remove(uid)
```

#### 4. Gang-Scheduling Retry Considerations
For gang-scheduled pods, the entire gang should be retried together:
- Track gang retry state separately
- Only retry when all pods in gang are present
- Use longer backoff for gangs (they require more resources)

### Implementation Strategy
1. Add a background thread that processes the retry queue every second
2. Maintain a separate queue for gang-scheduled pods
3. Implement cluster event listeners (node additions, pod deletions) to trigger immediate retries
4. Add metrics to track retry counts and success rates
5. Implement circuit breaker pattern to back off when cluster is consistently full

## Performance Optimization (Extra Points)

### Current Performance Characteristics
- **Time Complexity**: O(N) for finding preemption candidates (N = number of nodes)
- **Space Complexity**: O(N + P) where N = nodes, P = pods
- **Watch Loop**: Single-threaded event processing

### Recommended Optimizations for Large Scale

#### 1. Caching and Indexing
```python
class OptimizedScheduler:
    def __init__(self):
        # Index nodes by available capacity
        self.available_nodes = set()  # O(1) lookups
        
        # Priority queue for preemption candidates
        self.preemption_heap = []  # Min heap by priority
        
        # Index pods by priority for quick preemption decisions
        self.pods_by_priority = defaultdict(list)
```

**Benefits**:
- O(1) available node lookups instead of O(N)
- O(log N) preemption candidate selection using heap
- Reduced iteration over all nodes

#### 2. Batching Scheduling Decisions
Instead of processing pods one-by-one, batch similar scheduling decisions:
```python
def schedule_batch(self, pods, batch_size=10):
    """Process multiple pods in one transaction."""
    pods_by_priority = sorted(pods, key=lambda p: p.priority, reverse=True)
    
    for i in range(0, len(pods_by_priority), batch_size):
        batch = pods_by_priority[i:i+batch_size]
        self.schedule_pod_batch(batch)
```

**Benefits**:
- Reduce API server round trips
- Better utilize available capacity
- Lower scheduling latency for bursts

#### 3. Parallel Processing
For large clusters, parallelize independent operations:
```python
from concurrent.futures import ThreadPoolExecutor

def schedule_independent_pods(self, pods):
    """Schedule pods that don't have resource conflicts in parallel."""
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for pod in pods:
            if not pod.pod_group:  # Only parallelize non-gang pods
                futures.append(executor.submit(self.schedule_single_pod, pod))
        
        results = [f.result() for f in futures]
    return results
```

**Benefits**:
- Reduce total scheduling time for large pod batches
- Better CPU utilization
- Lower p99 latency

#### 4. Incremental State Updates
Avoid full cluster state refresh:
```python
def handle_node_event(self, event):
    """Incrementally update node state instead of full refresh."""
    event_type = event['type']
    node = event['object']
    
    if event_type == 'ADDED':
        self.node_assignments[node.metadata.name] = None
    elif event_type == 'DELETED':
        if node.metadata.name in self.node_assignments:
            del self.node_assignments[node.metadata.name]
    elif event_type == 'MODIFIED':
        # Update schedulability
        if node.spec.unschedulable:
            self.node_assignments.pop(node.metadata.name, None)
```

**Benefits**:
- O(1) updates instead of O(N) refresh
- Lower memory churn
- More responsive to cluster changes

#### 5. Event Filtering and Preprocessing
Reduce unnecessary processing:
```python
def should_process_event(self, event):
    """Filter events before processing."""
    pod = event['object']
    
    # Ignore pods not using our scheduler
    if pod.spec.scheduler_name != self.scheduler_name:
        return False
    
    # Ignore pods in terminal states
    if pod.status.phase in ['Succeeded', 'Failed']:
        return False
    
    return True
```

#### 6. Smart Preemption Strategy
Use locality and cost metrics:
```python
def find_best_preemption_candidates(self, required_priority, count):
    """Find best nodes for preemption considering multiple factors."""
    candidates = []
    
    for node_name, pod_uid in self.node_assignments.items():
        if pod_uid:
            pod = self.get_pod(pod_uid)
            priority = self._get_pod_priority(pod)
            
            if priority < required_priority:
                # Score based on priority difference (prefer larger difference)
                score = required_priority - priority
                candidates.append((score, node_name, pod))
    
    # Sort by score descending
    candidates.sort(reverse=True)
    return [(node, pod) for _, node, pod in candidates[:count]]
```

### Scalability Targets
With these optimizations, the scheduler should handle:
- **5,000+ nodes** with sub-second scheduling decisions
- **10,000+ pods** with minimal memory overhead (< 2GB)
- **100+ pods/second** throughput for batch scheduling
- **< 100ms p99 latency** for single pod scheduling

### Monitoring and Metrics
Essential metrics to track:
- Scheduling latency (p50, p95, p99)
- Preemption rate
- Gang-scheduling success rate
- Queue depth
- Failed scheduling attempts
- Node utilization

## Project Structure

```
k8s-playground/
├── README.md                    # This file
├── setup.sh                     # Deployment script
├── scheduler/
│   ├── scheduler.py            # Main scheduler implementation
│   ├── requirements.txt        # Python dependencies
│   └── Dockerfile              # Container image definition
├── k8s/
│   ├── rbac.yaml               # RBAC configuration
│   ├── deployment.yaml         # Scheduler deployment
│   └── example-deployments.yaml # Example workloads (with auto-recreation)
├── demo-preemption.sh          # Interactive preemption demo
└── tests/
    ├── test_scheduler.py       # Unit tests
    └── requirements.txt        # Test dependencies
```

## Troubleshooting

### Scheduler Not Starting
```bash
# Check scheduler pod status
kubectl get pods -n kube-system -l app=custom-scheduler

# Check logs
kubectl logs -n kube-system -l app=custom-scheduler

# Check RBAC permissions
kubectl auth can-i list pods --as=system:serviceaccount:kube-system:custom-scheduler
```

### Pods Not Being Scheduled
1. Verify `schedulerName: custom-scheduler` is set in pod spec
2. Check scheduler logs for errors
3. Ensure cluster has available nodes
4. Verify priority values are valid integers

### Gang-Scheduling Not Working
1. Check that all pods in gang have the same `pod-group` annotation
2. Ensure gang size doesn't exceed node count
3. Verify priorities are set correctly
4. Check scheduler logs for gang-scheduling attempts

## Cleanup

To remove the scheduler:
```bash
kubectl delete -f k8s/deployment.yaml
kubectl delete -f k8s/rbac.yaml
```

To delete the minikube cluster:
```bash
minikube delete -p custom-scheduler-demo
```

## License

MIT License - Feel free to use this for learning and production use.

## Contributing

Contributions are welcome! Areas for improvement:
- Add scheduling retry with exponential backoff
- Implement performance optimizations for large clusters
- Add Prometheus metrics
- Support for node affinity rules
- Integration with cluster autoscaler
- Advanced gang-scheduling with min/max group sizes

