"""
Unit tests for the custom scheduler logic (pure Python, no K8s mocking).
"""

import unittest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scheduler'))

from scheduling_logic import SchedulingLogic, PodInfo


class TestSchedulingLogic(unittest.TestCase):
    """Test cases for SchedulingLogic (pure Python)."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.logic = SchedulingLogic()
    
    def _create_pod_dict(self, uid, name, namespace="default", priority=0, 
                         gang_name=None, node_name=None):
        """Create a pod dictionary."""
        return {
            "uid": uid,
            "name": name,
            "namespace": namespace,
            "priority": priority,
            "gang_name": gang_name,
            "node_name": node_name
        }
    
    def test_initialize_empty_cluster(self):
        """Test initializing with no existing pods."""
        nodes = ["node-1", "node-2", "node-3"]
        result = self.logic.initialize(nodes, [])
        
        self.assertEqual(len(self.logic.node_assignments), 3)
        self.assertEqual(len(self.logic.all_pods), 0)
        self.assertEqual(len(result["actions"]), 0)
    
    def test_initialize_with_running_pods(self):
        """Test initializing with existing running pods."""
        nodes = ["node-1", "node-2", "node-3"]
        existing_pods = [
            self._create_pod_dict("uid-1", "pod-1", priority=50, node_name="node-1"),
            self._create_pod_dict("uid-2", "pod-2", priority=30, node_name="node-2"),
        ]
        
        result = self.logic.initialize(nodes, existing_pods)
        
        self.assertEqual(self.logic.node_assignments["node-1"], "uid-1")
        self.assertEqual(self.logic.node_assignments["node-2"], "uid-2")
        self.assertIsNone(self.logic.node_assignments["node-3"])
        self.assertEqual(len(self.logic.all_pods), 2)  # Both running pods are tracked
    
    def test_initialize_with_pending_pods(self):
        """Test initializing with pending pods."""
        nodes = ["node-1", "node-2"]
        existing_pods = [
            self._create_pod_dict("uid-1", "pod-1", priority=50),
        ]
        
        result = self.logic.initialize(nodes, existing_pods)
        
        # Should schedule the pending pod
        self.assertEqual(len(result["actions"]), 1)
        self.assertEqual(result["actions"][0]["action"], "bind")
        self.assertEqual(result["actions"][0]["pod_uid"], "uid-1")
    
    def test_add_pod_event(self):
        """Test handling ADDED event for a pod."""
        self.logic.initialize(["node-1", "node-2"], [])
        
        event = {
            "event_type": "ADDED",
            "pod": self._create_pod_dict("uid-1", "pod-1", priority=50)
        }
        
        result = self.logic.handle_event(event)
        
        # Should bind the pod
        self.assertEqual(len(result["actions"]), 1)
        self.assertEqual(result["actions"][0]["action"], "bind")
        self.assertEqual(result["actions"][0]["pod_uid"], "uid-1")
    
    def test_delete_pod_event(self):
        """Test handling DELETED event for a pod."""
        # Setup: initialize with a running pod
        nodes = ["node-1"]
        existing_pods = [
            self._create_pod_dict("uid-1", "pod-1", priority=50, node_name="node-1")
        ]
        self.logic.initialize(nodes, existing_pods)
        
        # Delete the pod
        event = {
            "event_type": "DELETED",
            "pod": self._create_pod_dict("uid-1", "pod-1", priority=50, node_name="node-1")
        }
        
        result = self.logic.handle_event(event)
        
        # Node should be freed
        self.assertIsNone(self.logic.node_assignments["node-1"])
    
    def test_priority_based_scheduling(self):
        """Test that higher priority pods are scheduled first."""
        self.logic.initialize(["node-1"], [])
        
        # Add low priority pod first
        event1 = {
            "event_type": "ADDED",
            "pod": self._create_pod_dict("uid-low", "low-pod", priority=10)
        }
        result1 = self.logic.handle_event(event1)
        
        # Low priority pod should be scheduled
        self.assertEqual(len(result1["actions"]), 1)
        self.assertEqual(result1["actions"][0]["pod_uid"], "uid-low")
        
        # Simulate binding completion
        self.logic.node_assignments["node-1"] = "uid-low"
        
        # Add high priority pod
        event2 = {
            "event_type": "ADDED",
            "pod": self._create_pod_dict("uid-high", "high-pod", priority=100)
        }
        result2 = self.logic.handle_event(event2)
        
        # Should preempt low priority and bind high priority
        self.assertEqual(len(result2["actions"]), 2)
        preempt_action = next(a for a in result2["actions"] if a["action"] == "preempt")
        bind_action = next(a for a in result2["actions"] if a["action"] == "bind")
        
        self.assertEqual(preempt_action["pod_uid"], "uid-low")
        self.assertEqual(bind_action["pod_uid"], "uid-high")
    
    def test_gang_scheduling_with_capacity(self):
        """Test gang-scheduling when enough nodes are available."""
        self.logic.initialize(["node-1", "node-2", "node-3"], [])
        
        # Add gang pods
        gang_pods = [
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-1", "gang-1", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-2", "gang-2", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-3", "gang-3", priority=50, gang_name="group-a")},
        ]
        
        # Process all gang pods and collect all actions
        all_bind_actions = []
        for event in gang_pods:
            result = self.logic.handle_event(event)
            bind_actions = [a for a in result["actions"] if a["action"] == "bind"]
            all_bind_actions.extend(bind_actions)
        
        # All gang pods should be scheduled (cumulative across events)
        self.assertEqual(len(all_bind_actions), 3)
        scheduled_uids = [a["pod_uid"] for a in all_bind_actions]
        self.assertEqual(set(scheduled_uids), {"uid-1", "uid-2", "uid-3"})
    
    def test_gang_scheduling_insufficient_capacity(self):
        """Test gang-scheduling fails when insufficient capacity."""
        # Only 2 nodes available
        self.logic.initialize(["node-1", "node-2"], [])
        
        # Try to add a gang of 3 pods
        gang_pods = [
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-1", "gang-1", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-2", "gang-2", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-3", "gang-3", priority=50, gang_name="group-a")},
        ]
        
        for event in gang_pods:
            result = self.logic.handle_event(event)
        
        # No gang pods should be scheduled (all-or-nothing)
        bind_actions = [a for a in result["actions"] if a["action"] == "bind"]
        gang_binds = [a for a in bind_actions if a["pod_uid"] in ["uid-1", "uid-2", "uid-3"]]
        self.assertEqual(len(gang_binds), 0, "Gang should not be scheduled without enough capacity")
    
    def test_gang_scheduling_with_priority(self):
        """Test gang-scheduling respects priority (uses minimum priority)."""
        self.logic.initialize(["node-1", "node-2"], [])
        
        # Add gang with mixed priorities (min priority = 30)
        gang_pods = [
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-1", "gang-1", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-2", "gang-2", priority=30, gang_name="group-a")},
        ]
        
        for event in gang_pods:
            self.logic.handle_event(event)
        
        # Add a single pod with priority 40 (higher than gang's min priority)
        event_high = {
            "event_type": "ADDED",
            "pod": self._create_pod_dict("uid-high", "high-pod", priority=40)
        }
        result = self.logic.handle_event(event_high)
        
        # High priority single pod should be scheduled before gang
        bind_actions = [a for a in result["actions"] if a["action"] == "bind"]
        scheduled_uids = [a["pod_uid"] for a in bind_actions]
        
        # High priority pod should be scheduled
        self.assertIn("uid-high", scheduled_uids)
    
    def test_multiple_gangs(self):
        """Test multiple gang groups are handled independently."""
        self.logic.initialize(["node-1", "node-2", "node-3", "node-4"], [])
        
        # Add gang A (2 pods)
        gang_a = [
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-a1", "gang-a1", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-a2", "gang-a2", priority=50, gang_name="group-a")},
        ]
        
        # Add gang B (2 pods)
        gang_b = [
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-b1", "gang-b1", priority=40, gang_name="group-b")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-b2", "gang-b2", priority=40, gang_name="group-b")},
        ]
        
        all_bind_actions = []
        for event in gang_a + gang_b:
            result = self.logic.handle_event(event)
            bind_actions = [a for a in result["actions"] if a["action"] == "bind"]
            all_bind_actions.extend(bind_actions)
        
        # All 4 pods should be scheduled (2 gangs, 2 pods each) - cumulative
        self.assertEqual(len(all_bind_actions), 4)
    
    def test_rebuild_scheduling_queue(self):
        """Test scheduling queue is rebuilt correctly."""
        # Use a cluster with only 1 node to keep some pods pending
        self.logic.initialize(["node-1"], [])
        
        # Add pods with different priorities
        events = [
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-low", "low-pod", priority=10)},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-high", "high-pod", priority=100)},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-mid", "mid-pod", priority=50)},
        ]
        
        # Add all pods (only highest priority will be scheduled due to capacity)
        for event in events:
            self.logic.handle_event(event)
        
        # After scheduling, all pods are tracked in all_pods
        self.assertEqual(len(self.logic.all_pods), 3, "All 3 pods should be tracked")
        
        # Verify scheduling happened based on priority - only 1 node available
        # Check node_assignments to see which pod got the node
        assigned_pods = [uid for node, uid in self.logic.node_assignments.items() if uid is not None]
        self.assertEqual(len(assigned_pods), 1, "Only 1 pod should be assigned (1 node available)")
        self.assertIn("uid-high", assigned_pods, "Highest priority pod should be assigned")
    
    def test_preemption_for_single_pod(self):
        """Test preemption works for single pods."""
        # Setup: node-1 has a low priority pod
        nodes = ["node-1"]
        existing_pods = [
            self._create_pod_dict("uid-low", "low-pod", priority=10, node_name="node-1")
        ]
        self.logic.initialize(nodes, existing_pods)
        
        # Add high priority pod
        event = {
            "event_type": "ADDED",
            "pod": self._create_pod_dict("uid-high", "high-pod", priority=100)
        }
        result = self.logic.handle_event(event)
        
        # Should have preempt and bind actions
        self.assertEqual(len(result["actions"]), 2)
        
        actions_by_type = {a["action"]: a for a in result["actions"]}
        self.assertIn("preempt", actions_by_type)
        self.assertIn("bind", actions_by_type)
        
        self.assertEqual(actions_by_type["preempt"]["pod_uid"], "uid-low")
        self.assertEqual(actions_by_type["bind"]["pod_uid"], "uid-high")
    
    def test_stable_scheduling_no_unnecessary_moves(self):
        """Test that pods don't move unnecessarily when higher priority pod arrives with available capacity."""
        # Setup: 3 nodes, 2 low priority pods already scheduled
        nodes = ["node-1", "node-2", "node-3"]
        existing_pods = [
            self._create_pod_dict("uid-low-1", "low-1", priority=10, node_name="node-1"),
            self._create_pod_dict("uid-low-2", "low-2", priority=10, node_name="node-2"),
        ]
        self.logic.initialize(nodes, existing_pods)
        
        # Verify initial state
        self.assertEqual(self.logic.node_assignments["node-1"], "uid-low-1")
        self.assertEqual(self.logic.node_assignments["node-2"], "uid-low-2")
        self.assertIsNone(self.logic.node_assignments["node-3"])
        
        # Add high priority pod (should go to node-3, not move existing pods)
        event = {
            "event_type": "ADDED",
            "pod": self._create_pod_dict("uid-high", "high-pod", priority=100)
        }
        result = self.logic.handle_event(event)
        
        # Verify actions - should ONLY bind high pod to node-3, NO preemptions
        self.assertEqual(len(result["actions"]), 1, "Should only have 1 action (bind high pod)")
        
        action = result["actions"][0]
        self.assertEqual(action["action"], "bind")
        self.assertEqual(action["pod_uid"], "uid-high")
        self.assertEqual(action["node_name"], "node-3")
        
        # Verify final state - low priority pods should NOT have moved (STABILITY!)
        self.assertEqual(self.logic.node_assignments["node-1"], "uid-low-1", 
                        "low-1 should stay on node-1 (no unnecessary move)")
        self.assertEqual(self.logic.node_assignments["node-2"], "uid-low-2",
                        "low-2 should stay on node-2 (no unnecessary move)")
        self.assertEqual(self.logic.node_assignments["node-3"], "uid-high",
                        "high should be on node-3")
    
    def test_stable_scheduling_with_preemption_minimizes_moves(self):
        """Test that when preemption is needed, only necessary pods are moved."""
        # Setup: 2 nodes FULL with low priority pods
        nodes = ["node-1", "node-2"]
        existing_pods = [
            self._create_pod_dict("uid-low-1", "low-1", priority=10, node_name="node-1"),
            self._create_pod_dict("uid-low-2", "low-2", priority=10, node_name="node-2"),
        ]
        self.logic.initialize(nodes, existing_pods)
        
        # Add high priority pod (cluster full - preemption needed)
        event = {
            "event_type": "ADDED",
            "pod": self._create_pod_dict("uid-high", "high-pod", priority=100)
        }
        result = self.logic.handle_event(event)
        
        # Verify: should preempt ONE pod, bind high pod
        # At least one low priority pod should be preempted
        preempt_actions = [a for a in result["actions"] if a["action"] == "preempt"]
        bind_actions = [a for a in result["actions"] if a["action"] == "bind"]
        
        self.assertEqual(len(preempt_actions), 1, "Should preempt exactly 1 pod")
        self.assertEqual(len(bind_actions), 1, "Should bind exactly 1 pod (high)")
        
        # Verify one low priority pod stayed on its original node (stability)
        assigned_uids = set(self.logic.node_assignments.values())
        self.assertIn("uid-high", assigned_uids, "High priority pod should be scheduled")
        
        # At least one low priority pod should still be there
        low_pods_remaining = [uid for uid in assigned_uids if uid and uid.startswith("uid-low")]
        self.assertEqual(len(low_pods_remaining), 1, "One low priority pod should remain")
    
    def test_size_tiebreaker_smaller_units_first(self):
        """Test that when priorities are equal, smaller units are scheduled first."""
        # Setup: 3 nodes available
        nodes = ["node-1", "node-2", "node-3"]
        self.logic.initialize(nodes, [])
        
        # Add a gang of 3 pods (priority 50)
        for i in range(1, 4):
            event = {
                "event_type": "ADDED",
                "pod": self._create_pod_dict(f"gang-{i}", f"gang-{i}", priority=50, gang_name="big-gang")
            }
            self.logic.handle_event(event)
        
        # Add 2 single pods (also priority 50)
        for i in range(1, 3):
            event = {
                "event_type": "ADDED",
                "pod": self._create_pod_dict(f"single-{i}", f"single-{i}", priority=50)
            }
            result = self.logic.handle_event(event)
        
        # Verify scheduling queue order: smaller units should come first
        self.assertEqual(len(self.logic.scheduling_queue), 3, "Should have 3 units in queue")
        
        # Check that single pods (size 1) come before gang (size 3)
        queue_sizes = [len(unit.pods) for unit in self.logic.scheduling_queue]
        self.assertEqual(queue_sizes, [1, 1, 3], 
                        "Smaller units should be ordered before larger ones when priorities are equal")
        
        # Verify scheduling result: single pods should be scheduled, gang should not fit
        assigned_pods = [uid for uid in self.logic.node_assignments.values() if uid]
        self.assertEqual(len(assigned_pods), 2, "Only 2 pods should be scheduled (the single pods)")
        self.assertIn("single-1", assigned_pods, "single-1 should be scheduled")
        self.assertIn("single-2", assigned_pods, "single-2 should be scheduled")
        
        # Gang should not be scheduled (not enough capacity left)
        for i in range(1, 4):
            self.assertNotIn(f"gang-{i}", assigned_pods, f"gang-{i} should not be scheduled")


class TestSchedulingIntegration(unittest.TestCase):
    """Integration tests for complex scheduling scenarios."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.logic = SchedulingLogic()
    
    def _create_pod_dict(self, uid, name, namespace="default", priority=0, 
                         gang_name=None, node_name=None):
        """Create a pod dictionary."""
        return {
            "uid": uid,
            "name": name,
            "namespace": namespace,
            "priority": priority,
            "gang_name": gang_name,
            "node_name": node_name
        }
    
    def test_cluster_full_priority_scheduling(self):
        """Test priority-based scheduling when cluster is at capacity."""
        # Setup: 2 nodes with low priority pods
        nodes = ["node-1", "node-2"]
        existing_pods = [
            self._create_pod_dict("uid-low-1", "low-1", priority=10, node_name="node-1"),
            self._create_pod_dict("uid-low-2", "low-2", priority=10, node_name="node-2"),
        ]
        self.logic.initialize(nodes, existing_pods)
        
        # Add 3 high priority pods (only 2 should be scheduled)
        for i in range(1, 4):
            event = {
                "event_type": "ADDED",
                "pod": self._create_pod_dict(f"uid-high-{i}", f"high-{i}", priority=100)
            }
            result = self.logic.handle_event(event)
        
        # Should have actions for 2 pods (cluster capacity)
        bind_actions = [a for a in result["actions"] if a["action"] == "bind"]
        high_binds = [a for a in bind_actions if a["pod_uid"].startswith("uid-high")]
        self.assertLessEqual(len(high_binds), 2, "Only 2 pods can be scheduled (cluster capacity)")
    
    def test_gang_unschedulable_doesnt_block_others(self):
        """Test that an unschedulable gang doesn't block other pods."""
        # Setup: 2 nodes
        self.logic.initialize(["node-1", "node-2"], [])
        
        # Add a gang of 3 pods (needs 3 nodes, only 2 available)
        gang_events = [
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-gang-1", "gang-1", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-gang-2", "gang-2", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-gang-3", "gang-3", priority=50, gang_name="group-a")},
        ]
        
        for event in gang_events:
            self.logic.handle_event(event)
        
        # Add a higher priority single pod
        event_high = {
            "event_type": "ADDED",
            "pod": self._create_pod_dict("uid-high", "high-pod", priority=100)
        }
        result = self.logic.handle_event(event_high)
        
        # High priority pod should be scheduled (gang remains pending)
        bind_actions = [a for a in result["actions"] if a["action"] == "bind"]
        self.assertTrue(any(a["pod_uid"] == "uid-high" for a in bind_actions),
                       "High priority single pod should be scheduled despite unschedulable gang")
    
    def test_mixed_scheduling_scenario(self):
        """Test complex scenario with mixed single pods and gangs."""
        # Setup: 5 nodes
        self.logic.initialize([f"node-{i}" for i in range(1, 6)], [])
        
        # Add various pods:
        # - 2 low priority single pods
        # - 1 gang of 2 pods (medium priority)
        # - 1 high priority single pod
        events = [
            # Low priority singles
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-low-1", "low-1", priority=10)},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-low-2", "low-2", priority=10)},
            # Gang (medium priority)
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-gang-1", "gang-1", priority=50, gang_name="group-a")},
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-gang-2", "gang-2", priority=50, gang_name="group-a")},
            # High priority single
            {"event_type": "ADDED", "pod": self._create_pod_dict("uid-high", "high", priority=100)},
        ]
        
        # Process all events
        for event in events:
            result = self.logic.handle_event(event)
        
        # Check final state: all 5 pods should be tracked
        self.assertEqual(len(self.logic.all_pods), 5, "All 5 pods should be tracked")
        
        # Check node assignments: all 5 nodes should have pods assigned
        assigned_count = sum(1 for uid in self.logic.node_assignments.values() if uid is not None)
        self.assertEqual(assigned_count, 5, "All 5 nodes should have pods assigned")
        
        # Verify all pods got assigned
        assigned_uids = {uid for uid in self.logic.node_assignments.values() if uid is not None}
        expected_uids = {"uid-low-1", "uid-low-2", "uid-gang-1", "uid-gang-2", "uid-high"}
        self.assertEqual(assigned_uids, expected_uids, "All pod UIDs should be assigned to nodes")


if __name__ == '__main__':
    unittest.main()
