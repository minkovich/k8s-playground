#!/usr/bin/env python3
"""
Pure Python scheduling logic with no Kubernetes dependencies.
Implements priority-based scheduling, preemption, and gang-scheduling.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class PodInfo:
    """Information about a pod for scheduling decisions."""
    uid: str
    name: str
    namespace: str
    priority: int
    gang_name: Optional[str]
    waiting_on_deletion: bool = False


@dataclass
class SchedulingUnit:
    """
    Represents either a single pod or a gang of pods to be scheduled.
    Units are sorted by effective_priority for scheduling decisions.
    """
    pods: List[PodInfo]  # List of pods (single item for regular pod, multiple for gang)
    is_gang: bool
    effective_priority: int  # For gangs, this is the minimum priority
    gang_name: Optional[str] = None
    
    def __lt__(self, other):
        """Compare by priority (higher priority first), then by size (smaller first)."""
        # Higher priority first
        if self.effective_priority != other.effective_priority:
            return self.effective_priority > other.effective_priority
        # If priorities are equal, smaller units first (fewer pods)
        return len(self.pods) < len(other.pods)
    
    @property
    def required_nodes(self) -> int:
        """Number of nodes required for this unit."""
        return len(self.pods)


class SchedulingLogic:
    """
    Pure scheduling logic with no Kubernetes dependencies.
    
    Implements:
    - Priority-based scheduling (one pod per node)
    - Preemption of lower priority pods
    - Gang-scheduling for pod groups
    """
    
    def __init__(self):
        # Track available nodes: node_name -> pod_uid (None if available)
        self.node_assignments: Dict[str, Optional[str]] = {}
        
        # Track ALL non-terminal pods: pod_uid -> PodInfo
        self.all_pods: Dict[str, PodInfo] = {}
        
        # Ordered list of scheduling units (single pods or gangs) sorted by priority
        self.scheduling_queue: List[SchedulingUnit] = []
        
        # Track gangs that are in transition (being reformed after preemption)
        self.gangs_in_transition: Set[str] = set()
        
        logger.info("Scheduling logic initialized")
    
    def _get_pod_node(self, pod_uid: str) -> Optional[str]:
        """Get the node assigned to a pod, if any."""
        for node, uid in self.node_assignments.items():
            if uid == pod_uid:
                return node
        return None
    
    def initialize(self, nodes: List[str], existing_pods: List[dict]) -> dict:
        """
        Initialize the scheduler with current cluster state.
        
        Args:
            nodes: List of node names ["node-1", "node-2", "node-3"]
            existing_pods: List of pod dicts with keys:
                - uid: str
                - name: str
                - namespace: str
                - priority: int
                - gang_name: str | None
        
        Returns:
            Actions dict: {"actions": [{"action": "bind"|"preempt", ...}]}
        """
        logger.info(f"Initializing with {len(nodes)} nodes and {len(existing_pods)} existing pods")
        
        # Initialize node tracking
        self.node_assignments = {node: None for node in nodes}
        
        # Process existing pods - track ALL non-terminal pods
        for pod_dict in existing_pods:
            # Extract node_name before creating PodInfo (PodInfo doesn't have node_name field)
            node_name = pod_dict.get("node_name")
            
            # Create PodInfo (without node_name field)
            pod_info = PodInfo(
                uid=pod_dict["uid"],
                name=pod_dict["name"],
                namespace=pod_dict["namespace"],
                priority=pod_dict["priority"],
                gang_name=pod_dict.get("gang_name"),
                waiting_on_deletion=False  # Existing pods are not waiting on deletion
            )
            self.all_pods[pod_info.uid] = pod_info
            
            # Update node assignments for pods that are already assigned
            if node_name and node_name in self.node_assignments:
                self.node_assignments[node_name] = pod_info.uid
        
        logger.info(f"Initialized: {len(self.all_pods)} total pods, "
                   f"{sum(1 for v in self.node_assignments.values() if v)} assigned to nodes")
        
        # Create initial plan for pending pods
        plan = self._create_scheduling_plan()
        actions = self._plan_to_actions(plan)        
        return {"actions": actions}
    
    def handle_event(self, event: dict) -> dict:
        """
        Process a pod event and return scheduling actions.
        
        Args:
            event: Dict with keys:
                - event_type: "ADDED" | "DELETED"
                - pod: Dict with pod info (same format as initialize)
        
        Returns:
            Actions dict: {"actions": [{"action": "bind"|"preempt", ...}]}
        """
        event_type = event["event_type"]
        pod_dict = event["pod"]
        
        # Extract node_name before creating PodInfo (PodInfo doesn't have node_name field)
        node_name = pod_dict.get("node_name")
        
        # Create PodInfo (without node_name field)
        pod_info = PodInfo(
            uid=pod_dict["uid"],
            name=pod_dict["name"],
            namespace=pod_dict["namespace"],
            priority=pod_dict["priority"],
            gang_name=pod_dict.get("gang_name"),
            waiting_on_deletion=False  # New/modified pods are not waiting on deletion
        )
        
        logger.debug(f"Processing event {event_type} for pod {pod_info.namespace}/{pod_info.name} and node {node_name}")
        # print state of all_pods
        logger.debug(f"All pods: {self.all_pods}")
        logger.debug(f"Node assignments: {self.node_assignments}")

        if event_type == "DELETED":
            self._handle_deleted(pod_info)
        elif event_type == "MODIFIED": # verify they are all boring events
            # check if the pod is already in the all_pods .. this happens when it's pending deletetion
            if pod_info.uid not in self.all_pods:
                logger.debug(f"\tMODIFIED Pod {pod_info.namespace}/{pod_info.name} -> {node_name} pending deletion?")
                return {"actions": []}
            
            # if node_name exists and node assignment is different from the one in the pod, throw an error
            if node_name and self.node_assignments[node_name] != pod_info.uid:
                logger.error(f"MODIFIED Pod {pod_info.namespace}/{pod_info.name} is assigned to node {node_name} but the node assignment is {self.node_assignments[node_name]}")
                return {"actions": []}
            
            # if node_names doesn't exist, it's unexpected, throw an error
            if not node_name:
                logger.error(f"MODIFIED Pod {pod_info.namespace}/{pod_info.name} has no node_name")
                return {"actions": []}
        elif event_type == "ADDED":
            # Add/update the pod
            self.all_pods[pod_info.uid] = pod_info
            
            # Update node assignment if pod has one
            if node_name and node_name in self.node_assignments:
                self.node_assignments[node_name] = pod_info.uid
                logger.debug(f"Pod {pod_info.namespace}/{pod_info.name} added to node {node_name} and reverse lookup in all_pods: {self.all_pods[pod_info.uid]}")
            
            # Check if this completes a gang reformation
            if pod_info.gang_name and pod_info.gang_name in self.gangs_in_transition:
                self._check_gang_reformation_complete(pod_info.gang_name)
        
        # Create and return new plan
        plan = self._create_scheduling_plan()
        actions = self._plan_to_actions(plan)

        return {"actions": actions}
    
    def _check_gang_reformation_complete(self, gang_name: str):
        """
        Check if a gang in transition has completed reformation.
        A gang is complete when all members are present and none are waiting_on_deletion.
        """
        gang_pods = [p for p in self.all_pods.values() if p.gang_name == gang_name]
        
        # Check if any member is still waiting on deletion
        if any(p.waiting_on_deletion for p in gang_pods):
            logger.debug(f"Gang {gang_name} still has members waiting on deletion")
            return
        
        # Check if we have members (empty means all deleted, still in transition)
        if len(gang_pods) == 0:
            logger.debug(f"Gang {gang_name} has no members yet")
            return
        
        # Gang is complete - remove from transition
        self.gangs_in_transition.discard(gang_name)
        logger.info(f"Gang {gang_name} reformation complete with {len(gang_pods)} members")
    
    def _handle_deleted(self, pod_info: PodInfo):
        """Handle a pod deletion event."""
        # Remove from all_pods
        if pod_info.uid in self.all_pods:
            del self.all_pods[pod_info.uid]
        
        # Clear node assignment if it was scheduled
        for node, uid in self.node_assignments.items():
            if uid == pod_info.uid:
                self.node_assignments[node] = None
                logger.debug(f"Pod {pod_info.namespace}/{pod_info.name} deleted from node {node}")
                break
    
    
    def _rebuild_scheduling_queue(self):
        """Rebuild the scheduling queue with ALL pods as SchedulingUnits."""
        # Phase 1: Collect pods into groups (singles and gangs)
        single_pods = []
        gangs = {}  # gang_name -> list of PodInfo
        
        for pod_info in self.all_pods.values():
            # Skip pods waiting on deletion
            if pod_info.waiting_on_deletion:
                continue
            
            if pod_info.gang_name:
                # Gang pod - collect by gang_name
                if pod_info.gang_name not in gangs:
                    gangs[pod_info.gang_name] = []
                gangs[pod_info.gang_name].append(pod_info)
            else:
                # Single pod
                single_pods.append(pod_info)
        
        # Phase 2: Create SchedulingUnits from collected pods
        units = []
        
        # Create units for single pods
        for pod_info in single_pods:
            unit = SchedulingUnit(
                pods=[pod_info],
                is_gang=False,
                effective_priority=pod_info.priority
            )
            units.append(unit)
        
        # Create units for gangs
        for gang_name, gang_pods in gangs.items():
            # Skip gang if any member is waiting on deletion
            if any(p.waiting_on_deletion for p in gang_pods):
                logger.debug(f"Skipping gang {gang_name} - has members waiting on deletion")
                continue
            
            # Skip gang if it's in transition (being reformed after preemption)
            if gang_name in self.gangs_in_transition:
                logger.debug(f"Skipping gang {gang_name} - in transition after preemption")
                continue
            
            min_priority = min(p.priority for p in gang_pods)
            unit = SchedulingUnit(
                pods=gang_pods,
                is_gang=True,
                effective_priority=min_priority,
                gang_name=gang_name
            )
            units.append(unit)
        
        # Sort by priority (higher first, then smaller units first)
        self.scheduling_queue = sorted(units)
        logger.debug(f"Rebuilt scheduling queue with {len(self.scheduling_queue)} units")
    
    def _create_scheduling_plan(self) -> List[SchedulingUnit]:
        """
        Create a scheduling plan: which units should be scheduled.
        
        This is a pure function that only checks capacity - it doesn't assign
        specific nodes. Returns priority-ordered list of units that fit.
        
        Returns: List[SchedulingUnit] - units that can fit in cluster
        """
        # Rebuild the scheduling queue before creating a plan 
        self._rebuild_scheduling_queue()

        plan = []
        nodes_needed = 0
        total_nodes = len(self.node_assignments)
        
        # Process scheduling units in priority order (queue is already sorted)
        for unit in self.scheduling_queue:
            # Check if we have enough total capacity for this unit
            if nodes_needed + unit.required_nodes <= total_nodes:
                plan.append(unit)
                nodes_needed += unit.required_nodes
            # else: not enough capacity, skip this and all lower priority units
        
        return plan
    
    def _plan_to_actions(self, plan: List[SchedulingUnit]) -> List[dict]:
        """
        Convert a scheduling plan to actions, preserving stable assignments.
        
        Args:
            plan: List[SchedulingUnit] - units that should be scheduled
        
        Returns:
            List of action dicts: [{"action": "bind"|"preempt", ...}]
        """
        actions = []
        
        # Step 1: Identify which pods are in the plan
        pods_in_plan = {pod.uid for unit in plan for pod in unit.pods}
        logger.debug(f"Pods in plan: {pods_in_plan}")
        
        # Step 2: Preempt pods not in plan (lower priority or no longer exist)
        preempted_pods = []
        logger.debug(f"Node assignments: {self.node_assignments}")
        for node, pod_uid in list(self.node_assignments.items()):
            if pod_uid and pod_uid not in pods_in_plan:
                pod_info = self.all_pods.get(pod_uid)
                if pod_info:
                    actions.append({
                        "action": "preempt",
                        "pod_uid": pod_uid,
                        "pod_name": pod_info.name,
                        "pod_namespace": pod_info.namespace
                    })
                    preempted_pods.append(pod_uid)
                    
                    # Mark pod as waiting on deletion
                    self.all_pods[pod_uid].waiting_on_deletion = True
                    
                    # If this is a gang member, mark the gang as in transition
                    if pod_info.gang_name:
                        self.gangs_in_transition.add(pod_info.gang_name)
                        logger.debug(f"Gang {pod_info.gang_name} marked as in-transition")
                else:
                    logger.error(f"Pod {pod_uid} not found in all_pods")
                self.node_assignments[node] = None  # Free the node
                
        logger.debug(f"Preempted pods: {preempted_pods}")

        # Step 3: Find available nodes (not assigned or just freed)
        available_nodes = sorted([node for node, uid in self.node_assignments.items() if uid is None])
        logger.debug(f"Available nodes: {available_nodes}")

        # Step 4: Assign pods that need nodes (preserve existing valid assignments)
        for unit in plan:
            for pod in unit.pods:
                # Check if pod already has a valid assignment
                assigned_node = self._get_pod_node(pod.uid)
                if assigned_node:
                    # Pod is already correctly assigned, keep it (no need to check again - _get_pod_node already verified it)
                    logger.debug(f"Pod {pod.uid} is already correctly assigned to node {assigned_node}")
                    continue
                
                # Pod needs assignment (or reassignment)
                if not available_nodes:
                    logger.error(f"No available nodes for pod {pod.uid} - capacity calculation error")
                    break
                
                node = available_nodes.pop(0)
                
                # Check if we need to preempt current occupant (shouldn't happen after step 2)
                # It's better to fail here than to preempt a pod that should be kept.
                if self.node_assignments[node]:
                    logger.error(f"Logic error: node {node} is already assigned to pod {self.node_assignments[node]}")
                    break
                
                # Bind pod to node
                actions.append({
                    "action": "bind",
                    "pod_uid": pod.uid,
                    "pod_name": pod.name,
                    "pod_namespace": pod.namespace,
                    "node_name": node
                })
                
                # Optimistically update state
                self.node_assignments[node] = pod.uid
        
        return actions

