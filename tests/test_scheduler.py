"""
Unit tests for the custom Kubernetes scheduler.
"""

import unittest
from unittest.mock import MagicMock, Mock, patch, call
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scheduler'))

from scheduler import CustomScheduler, PodInfo


class TestCustomScheduler(unittest.TestCase):
    """Test cases for CustomScheduler."""
    
    def setUp(self):
        """Set up test fixtures."""
        with patch('scheduler.client'):
            self.scheduler = CustomScheduler(scheduler_name="test-scheduler")
            self.scheduler.v1 = MagicMock()
    
    def _create_mock_pod(self, name, namespace="default", priority=0, 
                         pod_group=None, uid=None, node_name=None):
        """Create a mock pod object."""
        pod = MagicMock()
        pod.metadata.name = name
        pod.metadata.namespace = namespace
        pod.metadata.uid = uid or f"uid-{name}"
        pod.metadata.annotations = {}
        
        if priority != 0:
            pod.metadata.annotations["priority"] = str(priority)
        if pod_group:
            pod.metadata.annotations["pod-group"] = pod_group
        
        pod.spec.priority = priority
        pod.spec.scheduler_name = "test-scheduler"
        pod.spec.node_name = node_name
        pod.status.phase = "Pending"
        
        return pod
    
    def _create_mock_node(self, name, schedulable=True):
        """Create a mock node object."""
        node = MagicMock()
        node.metadata.name = name
        node.spec.unschedulable = not schedulable
        return node
    
    def test_initialize_cluster_state(self):
        """Test cluster state initialization."""
        # Setup mock nodes
        nodes = MagicMock()
        nodes.items = [
            self._create_mock_node("node-1"),
            self._create_mock_node("node-2"),
            self._create_mock_node("node-3"),
        ]
        self.scheduler.v1.list_node.return_value = nodes
        
        # Setup mock pods
        pods = MagicMock()
        pod1 = self._create_mock_pod("pod1", uid="uid1", node_name="node-1")
        pods.items = [pod1]
        self.scheduler.v1.list_pod_for_all_namespaces.return_value = pods
        
        # Initialize
        self.scheduler.initialize_cluster_state()
        
        # Verify
        self.assertEqual(len(self.scheduler.node_assignments), 3)
        self.assertEqual(self.scheduler.node_assignments["node-1"], "uid1")
        self.assertIsNone(self.scheduler.node_assignments["node-2"])
        self.assertIsNone(self.scheduler.node_assignments["node-3"])
    
    def test_get_pod_priority(self):
        """Test pod priority extraction."""
        # Test annotation priority
        pod1 = self._create_mock_pod("pod1", priority=100)
        self.assertEqual(self.scheduler._get_pod_priority(pod1), 100)
        
        # Test default priority
        pod2 = self._create_mock_pod("pod2")
        self.assertEqual(self.scheduler._get_pod_priority(pod2), 0)
    
    def test_get_pod_group(self):
        """Test pod group extraction."""
        # Test with pod group
        pod1 = self._create_mock_pod("pod1", pod_group="group-a")
        self.assertEqual(self.scheduler._get_pod_group(pod1), "group-a")
        
        # Test without pod group
        pod2 = self._create_mock_pod("pod2")
        self.assertIsNone(self.scheduler._get_pod_group(pod2))
    
    def test_find_available_nodes(self):
        """Test finding available nodes."""
        self.scheduler.node_assignments = {
            "node-1": "uid1",
            "node-2": None,
            "node-3": None,
            "node-4": "uid4",
        }
        
        available = self.scheduler._find_available_nodes(2)
        self.assertEqual(len(available), 2)
        self.assertIn("node-2", available)
        self.assertIn("node-3", available)
    
    def test_find_nodes_for_preemption(self):
        """Test finding nodes for preemption based on priority."""
        # Setup node assignments
        self.scheduler.node_assignments = {
            "node-1": "uid1",  # priority 10
            "node-2": "uid2",  # priority 20
            "node-3": "uid3",  # priority 5
        }
        
        # Mock pod retrieval - field_selector is by spec.nodeName
        def mock_list_pods(field_selector):
            node_name = field_selector.split("=")[1]
            pods = MagicMock()
            if node_name == "node-1":
                pods.items = [self._create_mock_pod("pod1", priority=10, uid="uid1", node_name="node-1")]
            elif node_name == "node-2":
                pods.items = [self._create_mock_pod("pod2", priority=20, uid="uid2", node_name="node-2")]
            elif node_name == "node-3":
                pods.items = [self._create_mock_pod("pod3", priority=5, uid="uid3", node_name="node-3")]
            else:
                pods.items = []
            return pods
        
        self.scheduler.v1.list_pod_for_all_namespaces.side_effect = mock_list_pods
        
        # Find nodes for a pod with priority 15
        candidates = self.scheduler._find_nodes_for_preemption(required_priority=15, count=2)
        
        # Should return lowest priority pods first
        self.assertEqual(len(candidates), 2)
        self.assertEqual(candidates[0][0], "node-3")  # priority 5
        self.assertEqual(candidates[1][0], "node-1")  # priority 10
    
    def test_bind_pod_to_node_success(self):
        """Test successful pod binding to node."""
        pod = self._create_mock_pod("test-pod", priority=50)
        self.scheduler.node_assignments = {"node-1": None}
        
        # Mock successful binding
        self.scheduler.v1.create_namespaced_binding.return_value = None
        
        result = self.scheduler._bind_pod_to_node(pod, "node-1")
        
        self.assertTrue(result)
        self.assertEqual(self.scheduler.node_assignments["node-1"], "uid-test-pod")
        self.scheduler.v1.create_namespaced_binding.assert_called_once()
    
    def test_schedule_single_pod_with_available_node(self):
        """Test scheduling a single pod when nodes are available."""
        pod = self._create_mock_pod("test-pod", priority=50)
        pod_info = PodInfo(
            name="test-pod",
            namespace="default",
            priority=50,
            pod_group=None,
            uid="uid-test-pod"
        )
        
        self.scheduler.node_assignments = {"node-1": None, "node-2": None}
        self.scheduler.v1.create_namespaced_binding.return_value = None
        
        result = self.scheduler._schedule_single_pod(pod_info, pod)
        
        self.assertTrue(result)
        # One node should now be assigned
        assigned = sum(1 for v in self.scheduler.node_assignments.values() if v is not None)
        self.assertEqual(assigned, 1)
    
    def test_schedule_single_pod_with_preemption(self):
        """Test scheduling a pod with preemption."""
        # High priority pod to schedule
        high_priority_pod = self._create_mock_pod("high-pod", priority=100)
        pod_info = PodInfo(
            name="high-pod",
            namespace="default",
            priority=100,
            pod_group=None,
            uid="uid-high-pod"
        )
        
        # Low priority pod already on node
        low_priority_pod = self._create_mock_pod("low-pod", priority=10, 
                                                  uid="uid-low-pod", node_name="node-1")
        
        self.scheduler.node_assignments = {"node-1": "uid-low-pod"}
        
        # Mock pod retrieval for preemption
        pods_response = MagicMock()
        pods_response.items = [low_priority_pod]
        self.scheduler.v1.list_pod_for_all_namespaces.return_value = pods_response
        
        # Mock deletion and binding
        self.scheduler.v1.delete_namespaced_pod.return_value = None
        self.scheduler.v1.create_namespaced_binding.return_value = None
        
        with patch('scheduler.time.sleep'):
            result = self.scheduler._schedule_single_pod(pod_info, high_priority_pod)
        
        self.assertTrue(result)
        # Verify preemption was called
        self.scheduler.v1.delete_namespaced_pod.assert_called_once()
        # Verify new pod was bound
        self.scheduler.v1.create_namespaced_binding.assert_called_once()
    
    def test_gang_scheduling_with_available_nodes(self):
        """Test gang-scheduling when enough nodes are available."""
        pods_info = [
            PodInfo("gang-pod-1", "default", 50, "group-a", "uid1"),
            PodInfo("gang-pod-2", "default", 50, "group-a", "uid2"),
            PodInfo("gang-pod-3", "default", 50, "group-a", "uid3"),
        ]
        
        self.scheduler.node_assignments = {
            "node-1": None,
            "node-2": None,
            "node-3": None,
        }
        
        # Mock pod retrieval
        mock_pods = [
            self._create_mock_pod("gang-pod-1", uid="uid1"),
            self._create_mock_pod("gang-pod-2", uid="uid2"),
            self._create_mock_pod("gang-pod-3", uid="uid3"),
        ]
        self.scheduler.v1.read_namespaced_pod.side_effect = mock_pods
        self.scheduler.v1.create_namespaced_binding.return_value = None
        
        result = self.scheduler._schedule_gang_pods("group-a", pods_info)
        
        self.assertTrue(result)
        # All nodes should be assigned
        assigned = sum(1 for v in self.scheduler.node_assignments.values() if v is not None)
        self.assertEqual(assigned, 3)
    
    def test_gang_scheduling_with_preemption(self):
        """Test gang-scheduling with preemption."""
        pods_info = [
            PodInfo("gang-pod-1", "default", 100, "group-a", "uid-gang1"),
            PodInfo("gang-pod-2", "default", 100, "group-a", "uid-gang2"),
        ]
        
        # One node available, one occupied by lower priority pod
        low_priority_pod = self._create_mock_pod("low-pod", priority=10, 
                                                  uid="uid-low", node_name="node-2")
        
        self.scheduler.node_assignments = {
            "node-1": None,
            "node-2": "uid-low",
        }
        
        # Mock pod retrieval for preemption
        pods_response = MagicMock()
        pods_response.items = [low_priority_pod]
        self.scheduler.v1.list_pod_for_all_namespaces.return_value = pods_response
        
        # Mock gang pod retrieval
        mock_gang_pods = [
            self._create_mock_pod("gang-pod-1", priority=100, uid="uid-gang1"),
            self._create_mock_pod("gang-pod-2", priority=100, uid="uid-gang2"),
        ]
        self.scheduler.v1.read_namespaced_pod.side_effect = mock_gang_pods
        
        # Mock deletion and binding
        self.scheduler.v1.delete_namespaced_pod.return_value = None
        self.scheduler.v1.create_namespaced_binding.return_value = None
        
        with patch('scheduler.time.sleep'):
            result = self.scheduler._schedule_gang_pods("group-a", pods_info)
        
        self.assertTrue(result)
        # Verify preemption occurred
        self.scheduler.v1.delete_namespaced_pod.assert_called_once()
        # Verify both gang pods were bound
        self.assertEqual(self.scheduler.v1.create_namespaced_binding.call_count, 2)
    
    def test_gang_scheduling_insufficient_capacity(self):
        """Test gang-scheduling fails when insufficient capacity."""
        pods_info = [
            PodInfo("gang-pod-1", "default", 50, "group-a", "uid1"),
            PodInfo("gang-pod-2", "default", 50, "group-a", "uid2"),
            PodInfo("gang-pod-3", "default", 50, "group-a", "uid3"),
        ]
        
        # Only 1 node available, and 1 node with higher priority pod
        high_priority_pod = self._create_mock_pod("high-pod", priority=100, 
                                                   uid="uid-high", node_name="node-2")
        
        self.scheduler.node_assignments = {
            "node-1": None,
            "node-2": "uid-high",
        }
        
        # Mock pod retrieval - high priority pod cannot be preempted
        pods_response = MagicMock()
        pods_response.items = [high_priority_pod]
        self.scheduler.v1.list_pod_for_all_namespaces.return_value = pods_response
        
        result = self.scheduler._schedule_gang_pods("group-a", pods_info)
        
        # Should fail because we need 3 nodes but only have 1 available
        # and can't preempt the high priority pod
        self.assertFalse(result)
    
    def test_handle_pod_deleted(self):
        """Test handling pod deletion."""
        pod = self._create_mock_pod("test-pod", uid="uid-test", node_name="node-1")
        
        self.scheduler.node_assignments = {"node-1": "uid-test"}
        self.scheduler.handle_pod_deleted(pod)
        
        self.assertIsNone(self.scheduler.node_assignments["node-1"])
    
    def test_priority_ordering(self):
        """Test that higher priority pods preempt lower priority ones."""
        # Setup: node-1 has low priority pod
        low_pod = self._create_mock_pod("low-pod", priority=10, uid="uid-low", node_name="node-1")
        self.scheduler.node_assignments = {"node-1": "uid-low"}
        
        # High priority pod arrives
        high_pod = self._create_mock_pod("high-pod", priority=100, uid="uid-high-pod")
        pod_info = PodInfo("high-pod", "default", 100, None, "uid-high-pod")
        
        # Mock responses - need to mock by nodeName field selector
        def mock_list_pods(field_selector):
            node_name = field_selector.split("=")[1]
            pods = MagicMock()
            if node_name == "node-1":
                pods.items = [low_pod]
            else:
                pods.items = []
            return pods
        
        self.scheduler.v1.list_pod_for_all_namespaces.side_effect = mock_list_pods
        self.scheduler.v1.delete_namespaced_pod.return_value = None
        self.scheduler.v1.create_namespaced_binding.return_value = None
        
        with patch('scheduler.time.sleep'):
            result = self.scheduler._schedule_single_pod(pod_info, high_pod)
        
        self.assertTrue(result)
        # Low priority pod should have been deleted
        self.scheduler.v1.delete_namespaced_pod.assert_called_once_with(
            name="low-pod",
            namespace="default",
            body=unittest.mock.ANY
        )
        # High priority pod should be bound
        self.assertEqual(self.scheduler.node_assignments["node-1"], "uid-high-pod")


class TestSchedulerIntegration(unittest.TestCase):
    """Integration-style tests for scheduler behavior."""
    
    def setUp(self):
        """Set up test fixtures."""
        with patch('scheduler.client'):
            self.scheduler = CustomScheduler(scheduler_name="test-scheduler")
            self.scheduler.v1 = MagicMock()
    
    def _setup_cluster(self, num_nodes=3):
        """Setup a mock cluster with specified number of nodes."""
        self.scheduler.node_assignments = {
            f"node-{i}": None for i in range(1, num_nodes + 1)
        }
    
    def test_cluster_full_priority_scheduling(self):
        """Test priority-based scheduling when cluster is at capacity."""
        self._setup_cluster(num_nodes=2)
        
        # Fill cluster with low priority pods
        low_pods = [
            self._create_mock_pod(f"low-pod-{i}", priority=10, 
                                 uid=f"uid-low-{i}", node_name=f"node-{i}")
            for i in range(1, 3)
        ]
        
        self.scheduler.node_assignments = {
            "node-1": "uid-low-1",
            "node-2": "uid-low-2",
        }
        
        # High priority pods arrive
        high_pods = [
            self._create_mock_pod(f"high-pod-{i}", priority=100, uid=f"uid-high-{i}")
            for i in range(1, 4)  # 3 high priority pods
        ]
        
        # Mock responses for preemption - field_selector is by spec.nodeName
        def mock_list_pods(field_selector):
            node_name = field_selector.split("=")[1]
            pods = MagicMock()
            if node_name == "node-1":
                pods.items = [low_pods[0]]
            elif node_name == "node-2":
                pods.items = [low_pods[1]]
            else:
                pods.items = []
            return pods
        
        self.scheduler.v1.list_pod_for_all_namespaces.side_effect = mock_list_pods
        self.scheduler.v1.delete_namespaced_pod.return_value = None
        self.scheduler.v1.create_namespaced_binding.return_value = None
        
        # Schedule high priority pods
        scheduled_count = 0
        with patch('scheduler.time.sleep'):
            for i, high_pod in enumerate(high_pods):
                pod_info = PodInfo(f"high-pod-{i+1}", "default", 100, None, f"uid-high-{i+1}")
                if self.scheduler._schedule_single_pod(pod_info, high_pod):
                    scheduled_count += 1
        
        # Only 2 should be scheduled (cluster capacity = 2)
        self.assertEqual(scheduled_count, 2)
    
    def _create_mock_pod(self, name, namespace="default", priority=0, 
                         pod_group=None, uid=None, node_name=None):
        """Create a mock pod object."""
        pod = MagicMock()
        pod.metadata.name = name
        pod.metadata.namespace = namespace
        pod.metadata.uid = uid or f"uid-{name}"
        pod.metadata.annotations = {}
        
        if priority != 0:
            pod.metadata.annotations["priority"] = str(priority)
        if pod_group:
            pod.metadata.annotations["pod-group"] = pod_group
        
        pod.spec.priority = priority
        pod.spec.scheduler_name = "test-scheduler"
        pod.spec.node_name = node_name
        pod.status.phase = "Pending"
        
        return pod
    
    def test_gang_unschedulable_allows_other_pods(self):
        """Test that when a gang becomes unschedulable, low priority pods remain scheduled."""
        self._setup_cluster(num_nodes=2)
        
        # Mock responses for all operations
        self.scheduler.v1.create_namespaced_binding.return_value = None
        self.scheduler.v1.list_pod_for_all_namespaces.return_value = MagicMock(items=[])
        self.scheduler.v1.delete_namespaced_pod.return_value = None
        
        # Step 1: Schedule 2 low priority pods
        low_pod_1 = self._create_mock_pod("low-pod-1", priority=10, uid="uid-low-1")
        low_pod_info_1 = PodInfo("low-pod-1", "default", 10, None, "uid-low-1")
        
        low_pod_2 = self._create_mock_pod("low-pod-2", priority=10, uid="uid-low-2")
        low_pod_info_2 = PodInfo("low-pod-2", "default", 10, None, "uid-low-2")
        
        result1 = self.scheduler._schedule_single_pod(low_pod_info_1, low_pod_1)
        result2 = self.scheduler._schedule_single_pod(low_pod_info_2, low_pod_2)
        
        self.assertTrue(result1)
        self.assertTrue(result2)
        
        # Verify both low priority pods are scheduled
        scheduled_count = sum(1 for v in self.scheduler.node_assignments.values() if v in ["uid-low-1", "uid-low-2"])
        self.assertEqual(scheduled_count, 2, "Both low priority pods should be scheduled initially")
        
        # Step 2: Try to schedule a gang of 2 pods (should fail - no capacity)
        gang_pods_2 = [
            PodInfo("gang-pod-1", "default", 50, "group-a", "uid-gang-1"),
            PodInfo("gang-pod-2", "default", 50, "group-a", "uid-gang-2"),
        ]
        
        mock_gang_2 = [
            self._create_mock_pod("gang-pod-1", priority=50, uid="uid-gang-1", pod_group="group-a"),
            self._create_mock_pod("gang-pod-2", priority=50, uid="uid-gang-2", pod_group="group-a"),
        ]
        self.scheduler.v1.read_namespaced_pod.side_effect = mock_gang_2
        
        gang_result_2 = self.scheduler._schedule_gang_pods("group-a", gang_pods_2)
        self.assertFalse(gang_result_2, "Gang of 2 pods should fail to schedule (no capacity)")
        
        # Step 3: Add a 3rd pod to the gang (making it need 3 nodes)
        gang_pods_3 = [
            PodInfo("gang-pod-1", "default", 50, "group-a", "uid-gang-1"),
            PodInfo("gang-pod-2", "default", 50, "group-a", "uid-gang-2"),
            PodInfo("gang-pod-3", "default", 50, "group-a", "uid-gang-3"),
        ]
        
        mock_gang_3 = [
            self._create_mock_pod("gang-pod-1", priority=50, uid="uid-gang-1", pod_group="group-a"),
            self._create_mock_pod("gang-pod-2", priority=50, uid="uid-gang-2", pod_group="group-a"),
            self._create_mock_pod("gang-pod-3", priority=50, uid="uid-gang-3", pod_group="group-a"),
        ]
        self.scheduler.v1.read_namespaced_pod.side_effect = mock_gang_3
        
        gang_result_3 = self.scheduler._schedule_gang_pods("group-a", gang_pods_3)
        self.assertFalse(gang_result_3, "Gang of 3 pods should fail to schedule (needs 3 nodes, only 2 available)")
        
        # Step 4: Verify the two low priority pods are still scheduled
        scheduled_low_priority = sum(1 for v in self.scheduler.node_assignments.values() if v in ["uid-low-1", "uid-low-2"])
        self.assertEqual(scheduled_low_priority, 2, "Both low priority pods should remain scheduled after gang failure")
        
        # Verify gang pods are NOT scheduled
        scheduled_gang = sum(1 for v in self.scheduler.node_assignments.values() if v in ["uid-gang-1", "uid-gang-2", "uid-gang-3"])
        self.assertEqual(scheduled_gang, 0, "No gang pods should be scheduled")


if __name__ == '__main__':
    unittest.main()

