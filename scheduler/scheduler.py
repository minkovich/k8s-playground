#!/usr/bin/env python3
"""
Custom Kubernetes Scheduler with Priority-based Scheduling, Preemption, and Gang-scheduling.
"""

import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PodInfo:
    """Information about a pod for scheduling decisions."""
    name: str
    namespace: str
    priority: int
    pod_group: Optional[str]
    uid: str


class CustomScheduler:
    """
    Custom Kubernetes scheduler implementing:
    - Priority-based scheduling (one pod per node)
    - Preemption of lower priority pods
    - Gang-scheduling for pod groups
    """
    
    def __init__(self, scheduler_name: str = "custom-scheduler"):
        self.scheduler_name = scheduler_name
        self.v1 = client.CoreV1Api()
        
        # Track node assignments: node_name -> pod_uid
        self.node_assignments: Dict[str, Optional[str]] = {}
        
        # Track pod groups: group_name -> set of pod_uids
        self.pod_groups: Dict[str, Set[str]] = defaultdict(set)
        
        # Track pending gang-scheduled pods: group_name -> list of PodInfo
        self.pending_gang_pods: Dict[str, List[PodInfo]] = defaultdict(list)
        
        # Queue of pending pods that couldn't be scheduled: (PodInfo, pod_object, retry_count)
        self.pending_pods_queue: deque = deque()
        
        # Track last time we processed pending queue
        self.last_pending_process_time = time.time()
        
        logger.info(f"Custom scheduler '{scheduler_name}' initialized")
    
    def initialize_cluster_state(self):
        """Initialize the scheduler's view of the cluster state."""
        logger.info("Initializing cluster state...")
        
        # Get all nodes
        nodes = self.v1.list_node()
        for node in nodes.items:
            # Check if node is schedulable
            if not node.spec.unschedulable:
                self.node_assignments[node.metadata.name] = None
        
        logger.info(f"Found {len(self.node_assignments)} schedulable nodes")
        
        # Get all running pods scheduled by us
        pods = self.v1.list_pod_for_all_namespaces()
        for pod in pods.items:
            if pod.spec.scheduler_name == self.scheduler_name and pod.spec.node_name:
                node_name = pod.spec.node_name
                if node_name in self.node_assignments:
                    self.node_assignments[node_name] = pod.metadata.uid
                    
                    # Track pod group membership
                    pod_group = self._get_pod_group(pod)
                    if pod_group:
                        self.pod_groups[pod_group].add(pod.metadata.uid)
        
        logger.info(f"Loaded {sum(1 for v in self.node_assignments.values() if v)} existing pod assignments")
    
    def _get_pod_group(self, pod) -> Optional[str]:
        """Extract pod group annotation from pod."""
        if pod.metadata.annotations:
            return pod.metadata.annotations.get("pod-group")
        return None
    
    def _get_pod_priority(self, pod) -> int:
        """Get pod priority. Higher number = higher priority."""
        # Check for priority annotation first
        if pod.metadata.annotations:
            priority_str = pod.metadata.annotations.get("priority")
            if priority_str:
                try:
                    return int(priority_str)
                except ValueError:
                    pass
        
        # Fall back to priorityClassName or default
        if pod.spec.priority is not None:
            return pod.spec.priority
        
        return 0  # Default priority
    
    def _get_pod_info(self, pod) -> PodInfo:
        """Extract relevant pod information."""
        return PodInfo(
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            priority=self._get_pod_priority(pod),
            pod_group=self._get_pod_group(pod),
            uid=pod.metadata.uid
        )
    
    def _find_available_nodes(self, count: int) -> List[str]:
        """Find available (unassigned) nodes."""
        available = [node for node, pod_uid in self.node_assignments.items() if pod_uid is None]
        return available[:count]
    
    def _find_nodes_for_preemption(self, required_priority: int, count: int) -> List[str]:
        """Find nodes with lower priority pods that can be preempted."""
        candidates = []
        
        for node_name, pod_uid in self.node_assignments.items():
            if pod_uid is None:
                continue
            
            # Get pods on this node (field_selector by spec.nodeName is supported)
            try:
                pods = self.v1.list_pod_for_all_namespaces(
                    field_selector=f"spec.nodeName={node_name}"
                )
                
                # Find the pod with matching UID
                for pod in pods.items:
                    if pod.metadata.uid == pod_uid:
                        pod_priority = self._get_pod_priority(pod)
                        
                        if pod_priority < required_priority:
                            candidates.append((node_name, pod_priority, pod))
                        break
                        
            except ApiException as e:
                logger.error(f"Error checking pod on node {node_name}: {e}")
                continue
        
        # Sort by priority (lowest first) to preempt lowest priority pods
        candidates.sort(key=lambda x: x[1])
        
        return [(node, pod) for node, _, pod in candidates[:count]]
    
    def _preempt_pod(self, pod):
        """
        Preempt a pod from its node.
        
        Note: In Kubernetes, spec.nodeName is immutable, so we must delete the pod.
        If the pod is managed by a controller (Deployment, ReplicaSet), it will be
        automatically recreated and become Pending again for rescheduling.
        """
        try:
            logger.info(f"Preempting pod {pod.metadata.namespace}/{pod.metadata.name} "
                       f"(priority: {self._get_pod_priority(pod)}) - pod will be deleted and recreated by its controller")
            
            # Update our tracking FIRST (before deletion)
            node_name = pod.spec.node_name
            if node_name in self.node_assignments:
                self.node_assignments[node_name] = None
            
            # Remove from pod group tracking
            pod_group = self._get_pod_group(pod)
            if pod_group and pod.metadata.uid in self.pod_groups[pod_group]:
                self.pod_groups[pod_group].remove(pod.metadata.uid)
            
            # Delete the pod (controller will recreate it if it exists)
            self.v1.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body=client.V1DeleteOptions(
                    grace_period_seconds=0  # Immediate deletion
                )
            )
            
            logger.info(f"Pod {pod.metadata.namespace}/{pod.metadata.name} deleted for preemption. "
                       f"If managed by a controller, it will be recreated as Pending")
            
            return True
        except ApiException as e:
            logger.error(f"Failed to preempt pod: {e}")
            return False
    
    def _bind_pod_to_node(self, pod, node_name: str) -> bool:
        """Bind a pod to a node using the binding subresource."""
        try:
            logger.info(f"Binding pod {pod.metadata.namespace}/{pod.metadata.name} to node {node_name}")
   
            target = client.V1ObjectReference(
                kind="Node",
                api_version="v1", 
                name=node_name
            )
            
            meta=client.V1ObjectMeta(
                name=pod.metadata.name
            )
            
            binding = client.V1Binding(
                target=target,
                metadata=meta
            )
    
            self.v1.create_namespaced_binding(
                namespace=pod.metadata.namespace, 
                body=binding,
                _preload_content=False
            )
            
            logger.info(f"Successfully bound pod {pod.metadata.namespace}/{pod.metadata.name} "
                       f"to node {node_name}")
            
            # Update our tracking
            self.node_assignments[node_name] = pod.metadata.uid
            
            # Track pod group membership
            pod_group = self._get_pod_group(pod)
            if pod_group:
                self.pod_groups[pod_group].add(pod.metadata.uid)
            
            return True
            
        except ApiException as e:
            logger.error(f"Failed to bind pod {pod.metadata.namespace}/{pod.metadata.name} "
                        f"to node {node_name}: {e}")
            return False
    
    def _schedule_single_pod(self, pod_info: PodInfo, pod) -> bool:
        """Schedule a single pod (non-gang-scheduled)."""
        # Try to find an available node
        available_nodes = self._find_available_nodes(1)
        
        if available_nodes:
            return self._bind_pod_to_node(pod, available_nodes[0])
        
        # No available nodes, try preemption
        logger.info(f"No available nodes for pod {pod_info.namespace}/{pod_info.name}, "
                   f"attempting preemption...")
        
        preemption_candidates = self._find_nodes_for_preemption(pod_info.priority, 1)
        
        if preemption_candidates:
            node_name, victim_pod = preemption_candidates[0]
            if self._preempt_pod(victim_pod):
                # Wait a moment for the preemption to process
                time.sleep(0.5)
                return self._bind_pod_to_node(pod, node_name)
        
        logger.warning(f"Cannot schedule pod {pod_info.namespace}/{pod_info.name} - "
                      f"no capacity and no preemption candidates")
        
        # Add to pending queue for retry later
        self._add_to_pending_queue(
            pod_info, pod,
            "No capacity and no preemption candidates"
        )
        
        return False
    
    def _schedule_gang_pods(self, pod_group: str, pods_info: List[PodInfo]) -> bool:
        """
        Schedule a gang of pods together (all or nothing).
        Returns True if all pods were scheduled successfully.
        """
        if not pods_info:
            return True
        
        required_count = len(pods_info)
        max_priority = max(p.priority for p in pods_info)
        
        logger.info(f"Attempting gang-scheduling for group '{pod_group}' "
                   f"({required_count} pods, max priority: {max_priority})")
        
        # Find available nodes
        available_nodes = self._find_available_nodes(required_count)
        
        if len(available_nodes) >= required_count:
            # We have enough nodes, bind all pods
            return self._bind_gang_pods(pods_info, available_nodes)
        
        # Not enough available nodes, try preemption
        needed = required_count - len(available_nodes)
        logger.info(f"Need to preempt {needed} pods for gang-scheduling")
        
        preemption_candidates = self._find_nodes_for_preemption(max_priority, needed)
        
        if len(preemption_candidates) >= needed:
            # Preempt the necessary pods
            preempted_nodes = []
            for node_name, victim_pod in preemption_candidates[:needed]:
                if self._preempt_pod(victim_pod):
                    preempted_nodes.append(node_name)
                else:
                    logger.error(f"Failed to preempt pod on node {node_name}")
                    return False
            
            # Wait for preemption to process
            time.sleep(0.5)
            
            # Combine available and preempted nodes
            all_nodes = available_nodes + preempted_nodes
            return self._bind_gang_pods(pods_info, all_nodes[:required_count])
        
        logger.warning(f"Cannot gang-schedule group '{pod_group}' - insufficient capacity. Unscheduling all mapped pods in the group.")
        
        # Unschedule any gang pods that are already bound to nodes (to maintain all-or-nothing)
        for pod_info in pods_info:
            # Check if this pod is already scheduled on a node
            node_name = None
            for node, uid in self.node_assignments.items():
                logger.info(f"Node: {node}, UID: {uid}, Pod UID: {pod_info.uid}, node name: {node_name}")
                if uid == pod_info.uid:
                    node_name = node
                    break
            
            if node_name:
                # Pod is already scheduled, need to preempt it
                try:
                    pod = self.v1.read_namespaced_pod(
                        name=pod_info.name,
                        namespace=pod_info.namespace
                    )
                    logger.info(f"Unscheduling gang pod {pod_info.namespace}/{pod_info.name} from node {node_name} "
                               f"(gang-scheduling failed for group '{pod_group}')")
                    self._preempt_pod(pod)
                except ApiException as e:
                    logger.error(f"Failed to fetch pod {pod_info.namespace}/{pod_info.name} for unscheduling: {e}")
        
        # Add gang pods to pending queue for retry later
        for pod_info in pods_info:
            try:
                pod = self.v1.read_namespaced_pod(
                    name=pod_info.name,
                    namespace=pod_info.namespace
                )
                self._add_to_pending_queue(
                    pod_info, pod,
                    f"Gang group '{pod_group}' needs {required_count} nodes, only {len(available_nodes)} available"
                )
            except ApiException as e:
                logger.error(f"Failed to fetch pod {pod_info.namespace}/{pod_info.name}: {e}")
        
        return False
    
    def _bind_gang_pods(self, pods_info: List[PodInfo], node_names: List[str]) -> bool:
        """Bind all pods in a gang to their assigned nodes."""
        # Fetch actual pod objects
        pods = []
        for pod_info in pods_info:
            try:
                pod = self.v1.read_namespaced_pod(
                    name=pod_info.name,
                    namespace=pod_info.namespace
                )
                pods.append(pod)
            except ApiException as e:
                logger.error(f"Failed to fetch pod {pod_info.namespace}/{pod_info.name}: {e}")
                return False
        
        # Bind all pods
        for pod, node_name in zip(pods, node_names):
            if not self._bind_pod_to_node(pod, node_name):
                # TODO: Rollback previous bindings on failure
                logger.error(f"Failed to bind pod in gang, scheduling may be inconsistent")
                return False
        
        logger.info(f"Successfully gang-scheduled {len(pods)} pods")
        return True
    
    def schedule_pod(self, pod):
        """Main scheduling logic for a pod."""
        pod_info = self._get_pod_info(pod)
        
        logger.info(f"Scheduling pod {pod_info.namespace}/{pod_info.name} "
                   f"(priority: {pod_info.priority}, group: {pod_info.pod_group})")
        
        # Check if this is a gang-scheduled pod
        if pod_info.pod_group:
            # Add to pending gang pods
            self.pending_gang_pods[pod_info.pod_group].append(pod_info)
            
            # Check if we have all pods in the group
            # We need to determine group size from annotation or wait for timeout
            # For simplicity, we'll use a heuristic: try to schedule after a short delay
            # In production, you'd use a "gang-size" annotation
            
            # For now, attempt to schedule all pending pods in this group
            self._schedule_gang_pods(
                pod_info.pod_group,
                self.pending_gang_pods[pod_info.pod_group]
            )
            
            # Clear pending pods for this group (whether successful or not)
            # In production, you'd want retry logic here
            if pod_info.pod_group in self.pending_gang_pods:
                del self.pending_gang_pods[pod_info.pod_group]
        else:
            # Regular single pod scheduling
            self._schedule_single_pod(pod_info, pod)
    
    def handle_pod_deleted(self, pod):
        """Handle a pod deletion event."""
        node_name = pod.spec.node_name
        if node_name and node_name in self.node_assignments:
            if self.node_assignments[node_name] == pod.metadata.uid:
                logger.info(f"Pod {pod.metadata.namespace}/{pod.metadata.name} "
                           f"deleted from node {node_name}")
                self.node_assignments[node_name] = None
        
        # Remove from pod group tracking
        pod_group = self._get_pod_group(pod)
        if pod_group and pod.metadata.uid in self.pod_groups[pod_group]:
            self.pod_groups[pod_group].remove(pod.metadata.uid)
        
        # After a pod is deleted, try to process pending queue
        self._process_pending_queue()
    
    def _add_to_pending_queue(self, pod_info: PodInfo, pod, reason: str = ""):
        """Add a pod to the pending queue for retry later."""
        # Check if pod is already in queue
        for i, (existing_info, _, _) in enumerate(self.pending_pods_queue):
            if existing_info.uid == pod_info.uid:
                # Already in queue, don't add again
                return
        
        self.pending_pods_queue.append((pod_info, pod, 0))  # retry_count = 0
        logger.info(f"Added pod {pod_info.namespace}/{pod_info.name} to pending queue. {reason}")
    
    def _process_pending_queue(self):
        """
        Process pending pods queue and try to schedule them.
        This allows unschedulable gang pods to not block other pods.
        """

        if not self.pending_pods_queue:
            return
        
        logger.info(f"Processing pending queue with {len(self.pending_pods_queue)} pods")
        
        # Process all pending pods (but limit iterations to prevent infinite loops)
        max_iterations = len(self.pending_pods_queue)
        for _ in range(max_iterations):
            if not self.pending_pods_queue:
                break
            
            pod_info, pod, retry_count = self.pending_pods_queue.popleft()
            
            # Try to schedule the pod
            success = False
            if pod_info.pod_group:
                # For gang pods, try gang scheduling
                success = self._schedule_gang_pods(
                    pod_info.pod_group,
                    self.pending_gang_pods.get(pod_info.pod_group, [pod_info])
                )
            else:
                # Regular pod
                success = self._schedule_single_pod(pod_info, pod)
            
            # If still can't schedule, add back to queue with increased retry count
            if not success:
                retry_count += 1
                if retry_count < 10:  # Max 10 retries before giving up for this round
                    self.pending_pods_queue.append((pod_info, pod, retry_count))
                else:
                    logger.warning(f"Pod {pod_info.namespace}/{pod_info.name} reached max retries, "
                                  f"will retry in next processing round")
    
    def run(self):
        """Main scheduler loop."""
        logger.info("Starting scheduler main loop...")
        self.initialize_cluster_state()
        
        w = watch.Watch()
        
        try:
            for event in w.stream(self.v1.list_pod_for_all_namespaces):
                event_type = event['type']
                pod = event['object']
                
                # Only process pods that use our scheduler
                if pod.spec.scheduler_name != self.scheduler_name:
                    continue
                
                logger.info(f"Event: {event_type} for pod {pod.metadata.namespace}/{pod.metadata.name}, "
                           f"phase={pod.status.phase}, node={pod.spec.node_name}")
            
                # Print all the state of the scheduler
                logger.info(f"Node assignments: {self.node_assignments}")
                logger.info(f"Pod groups: {self.pod_groups}")
                # Pending gang pods is a dict of group_name -> list of PodInfo
                pending_gang_uids = {group: [p.uid for p in pods] for group, pods in self.pending_gang_pods.items()}
                logger.info(f"Pending gang pods: {pending_gang_uids}")
                # Pending pods queue is a deque of (PodInfo, pod_object, retry_count) tuples
                pending_queue_info = [(pod_info.uid, pod_info.name, retry_count) for pod_info, pod, retry_count in self.pending_pods_queue]
                logger.info(f"Pending pods queue: {pending_queue_info}")

                if event_type in ['ADDED', 'MODIFIED']:
                    # Check if pod is pending and not yet assigned to a node
                    if pod.status.phase == 'Pending' and not pod.spec.node_name:
                        logger.info(f"Attempting to schedule pod {pod.metadata.namespace}/{pod.metadata.name}")
                        self.schedule_pod(pod)
                        
                        # After scheduling attempt, process pending queue
                        # This allows other pending pods to be scheduled if this one couldn't be
                        self._process_pending_queue()
                
                elif event_type == 'DELETED':
                    self.handle_pod_deleted(pod)
        
        except Exception as e:
            logger.error(f"Error in scheduler main loop: {e}", exc_info=True)
            raise


def main():
    """Main entry point."""
    try:
        # Load kubernetes config
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster config")
        except config.ConfigException:
            config.load_kube_config()
            logger.info("Loaded local kube config")
        
        # Use default-scheduler to replace the built-in scheduler
        import os
        scheduler_name = os.getenv("SCHEDULER_NAME", "default-scheduler")
        logger.info(f"Starting scheduler with name: {scheduler_name}")
        
        scheduler = CustomScheduler(scheduler_name=scheduler_name)
        scheduler.run()
    
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Scheduler failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

