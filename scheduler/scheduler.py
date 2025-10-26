#!/usr/bin/env python3
"""
Kubernetes Adapter for Custom Scheduler.
Handles all Kubernetes API interactions and delegates scheduling logic to scheduling_logic.py.
"""

import logging
import os
from typing import Optional

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

from scheduling_logic import SchedulingLogic

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CustomScheduler:
    """
    Kubernetes adapter for custom scheduler.
    Watches pod events, converts to simple dicts, and executes scheduling actions.
    """
    
    def __init__(self, scheduler_name: str = "custom-scheduler"):
        self.scheduler_name = scheduler_name
        self.v1 = client.CoreV1Api()
        self.logic = SchedulingLogic()
        self.last_reinit_time = 0  # Track when we last re-initialized
        self.reinit_cooldown = 30  # Minimum seconds between re-inits
        
        logger.info(f"Custom scheduler '{scheduler_name}' initialized")
    
    def initialize_cluster_state(self):
        """Initialize the scheduler's view of the cluster state."""
        logger.info("Initializing cluster state...")
        
        # Get all schedulable nodes
        nodes = self.v1.list_node()
        node_names = []
        for node in nodes.items:
            if not node.spec.unschedulable:
                node_names.append(node.metadata.name)
        
        logger.info(f"Found {len(node_names)} schedulable nodes")
        
        # Get all existing pods for our scheduler
        pods = self.v1.list_pod_for_all_namespaces()
        existing_pods = []
        for pod in pods.items:
            if pod.spec.scheduler_name == self.scheduler_name:
                # Filter out terminal phases
                if pod.status.phase in ['Succeeded', 'Failed']:
                    continue
                
                pod_dict = self._pod_to_dict(pod)
                existing_pods.append(pod_dict)
        
        logger.info(f"Found {len(existing_pods)} existing pods (Pending/Running)")
        
        # Initialize logic layer
        result = self.logic.initialize(node_names, existing_pods)
        
        # Execute any actions returned
        self._execute_actions(result["actions"])
    
    def _safe_reinitialize(self, reason: str) -> bool:
        """
        Safely re-initialize scheduler state after an error.
        Creates a fresh SchedulingLogic instance and re-reads cluster state.
        Includes cooldown to prevent infinite re-init loops.
        
        Args:
            reason: Description of why re-init was triggered
        
        Returns:
            True if re-init succeeded, False if skipped due to cooldown
        """
        import time
        current_time = time.time()
        
        # Check cooldown to prevent rapid re-init loops
        if current_time - self.last_reinit_time < self.reinit_cooldown:
            logger.warning(f"Skipping re-init (cooldown active): {reason}")
            return False
        
        try:
            logger.warning(f"Re-initializing scheduler state due to: {reason}")
            self.last_reinit_time = current_time
            
            # Wait for in-flight operations to complete and cluster state to settle
            time.sleep(10)
            
            # Create fresh SchedulingLogic instance (clears all state)
            self.logic = SchedulingLogic()
            
            # Re-read cluster state
            self.initialize_cluster_state()
            
            logger.info("Successfully re-initialized scheduler state")
            return True
        except Exception as e:
            logger.error(f"Failed to re-initialize scheduler state: {e}", exc_info=True)
            return False
    
    def _pod_to_dict(self, pod) -> dict:
        """Convert a Kubernetes pod object to a simple dict."""
        return {
            "uid": pod.metadata.uid,
            "name": pod.metadata.name,
            "namespace": pod.metadata.namespace,
            "priority": self._get_pod_priority(pod),
            "gang_name": self._get_pod_gang_name(pod),
            "node_name": pod.spec.node_name
        }
    
    def _get_pod_priority(self, pod) -> int:
        """Extract priority from pod annotations or spec."""
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
    
    def _get_pod_gang_name(self, pod) -> Optional[str]:
        """Extract gang name from pod annotations."""
        if pod.metadata.annotations:
            return pod.metadata.annotations.get("pod-group")
        return None
    
    def _execute_actions(self, actions: list):
        """Execute scheduling actions returned by logic layer."""
        if not actions:
            return
        
        logger.info(f"Executing {len(actions)} actions")
        
        for action in actions:
            action_type = action["action"]
            
            if action_type == "bind":
                self._execute_bind(
                    action["pod_name"],
                    action["pod_namespace"],
                    action["node_name"]
                )
            elif action_type == "preempt":
                self._execute_preempt(
                    action["pod_name"],
                    action["pod_namespace"]
                )
            else:
                logger.warning(f"Unknown action type: {action_type}")
    
    def _execute_bind(self, pod_name: str, pod_namespace: str, node_name: str) -> bool:
        """Bind a pod to a node using the Kubernetes binding API."""
        try:
            logger.info(f"Binding pod {pod_namespace}/{pod_name} to node {node_name}")
            
            # Create and submit binding
            binding = client.V1Binding(
                target=client.V1ObjectReference(kind="Node", api_version="v1", name=node_name),
                metadata=client.V1ObjectMeta(name=pod_name)
            )
            
            self.v1.create_namespaced_binding(
                namespace=pod_namespace,
                body=binding,
                _preload_content=False
            )
            
            logger.info(f"Successfully bound pod {pod_namespace}/{pod_name} to node {node_name}")
            return True
            
        except ApiException as e:
            if e.status == 404:
                # Pod not found - it was deleted. Notify logic layer to clean up state.
                logger.warning(f"Pod {pod_namespace}/{pod_name} not found (404) during bind - notifying logic of deletion")
                
                # We don't have full pod info, but we have enough for a DELETE event
                delete_event = {
                    "event_type": "DELETED",
                    "pod": {
                        "uid": "unknown",  # We don't have UID, but logic will search by name if needed
                        "name": pod_name,
                        "namespace": pod_namespace,
                        "priority": 0,
                        "gang_name": None
                    }
                }
                
                # Notify logic layer - don't execute resulting actions
                self.logic.handle_event(delete_event)
                
                return False  # Bind failed, but we handled it gracefully
            elif e.status == 409:
                # Conflict - pod may already be bound. Check if it's bound to our target node.
                logger.warning(f"Conflict (409) binding pod {pod_namespace}/{pod_name} to {node_name} - checking current state")
                
                try:
                    pod = self.v1.read_namespaced_pod(name=pod_name, namespace=pod_namespace)
                    
                    # Check if pod is already bound to the target node
                    if pod.spec.node_name == node_name:
                        logger.info(f"Pod {pod_namespace}/{pod_name} already bound to {node_name} - treating as success (idempotent)")
                        return True
                    else:
                        logger.error(f"Pod {pod_namespace}/{pod_name} bound to different node: {pod.spec.node_name} (expected {node_name})")
                        self._safe_reinitialize(f"bind conflict - pod on wrong node")
                        return False
                except ApiException as read_error:
                    logger.error(f"Failed to read pod after 409 conflict: {read_error}")
                    self._safe_reinitialize(f"bind conflict and failed to verify")
                    return False
            else:
                logger.error(f"Failed to bind pod {pod_namespace}/{pod_name} to node {node_name}: status:{e.status} {e}")
                self._safe_reinitialize(f"bind failure for {pod_namespace}/{pod_name}")
                return False
    
    def _execute_preempt(self, pod_name: str, pod_namespace: str) -> bool:
        """
        Preempt a pod by deleting it.
        
        Note: spec.nodeName is immutable, so we must delete the pod.
        If managed by a controller, it will be recreated and become Pending.
        """
        try:
            logger.info(f"Preempting pod {pod_namespace}/{pod_name} - deleting for recreation")
            
            # Delete the pod
            self.v1.delete_namespaced_pod(
                name=pod_name,
                namespace=pod_namespace,
                body=client.V1DeleteOptions(
                    grace_period_seconds=0  # Immediate deletion
                )
            )
            
            logger.info(f"Pod {pod_namespace}/{pod_name} deleted. "
                       f"Controller will recreate it as Pending if applicable")
            return True
            
        except ApiException as e:
            if e.status == 404:
                # Pod already deleted - this is actually success!
                logger.info(f"Pod {pod_namespace}/{pod_name} already deleted (404) - goal achieved")
                return True
            else:
                logger.error(f"Failed to preempt pod {pod_namespace}/{pod_name}: status:{e.status} {e}")
                self._safe_reinitialize(f"preempt failure for {pod_namespace}/{pod_name}")
                return False
    
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
                
                # Filter out terminal phases
                if pod.status.phase in ['Succeeded', 'Failed']:
                    logger.debug(f"Ignoring pod {pod.metadata.namespace}/{pod.metadata.name} "
                                f"in terminal phase {pod.status.phase}")
                    continue

                # TODO: Need to understand why these are coming in with old mapping information
                if event_type == "MODIFIED" and "Pending" in pod.status.phase:
                    logger.debug(f"Ignoring pod {pod.metadata.namespace}/{pod.metadata.name} "
                                f"in pending phase {pod.status.phase}")
                    continue
                
                logger.info(f"Event: {event_type} for pod {pod.metadata.namespace}/{pod.metadata.name}, "
                           f"node={pod.spec.node_name} phase={pod.status.phase}")
                
                # Convert to dict and send to logic layer
                event_dict = {
                    "event_type": event_type,
                    "pod": self._pod_to_dict(pod)
                }
                
                result = self.logic.handle_event(event_dict)
                
                # Execute actions
                self._execute_actions(result["actions"])
                
                # Log scheduler state
                logger.info(f"Nodes: {len(self.logic.node_assignments)}, "
                           f"Total pods: {len(self.logic.all_pods)}, "
                           f"Queue units: {len(self.logic.scheduling_queue)}")
        
        except Exception as e:
            logger.error(f"Error in scheduler main loop: {e}", exc_info=True)
            if self._safe_reinitialize(f"main loop exception: {e}"):
                logger.info("Recovered from error, continuing...")
                # Continue the loop after successful re-init
            else:
                logger.error("Failed to recover, shutting down")
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
        
        # Get scheduler name from environment
        scheduler_name = os.getenv("SCHEDULER_NAME", "custom-scheduler")
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
