# Kubernetes Event Debugging Reference

## Event Types

### ADDED Events
- `ADDED node=None phase=Pending` - New pod needs scheduling
- `ADDED node=<node> phase=Pending` - Pod already mapped (e.g., after scheduler restart)

### MODIFIED Events (Filtered by scheduler)
- `MODIFIED node=<node> phase=Pending` - Result of bind (ignored)
- `MODIFIED node=<node> phase=Running` - Pod started (ignored)
- `MODIFIED node=<node> phase=Succeeded` - Pod completed (ignored)

### DELETED Events
- `DELETED node=<node> phase=Succeeded` - Completed pod cleaned up
- `DELETED node=None phase=Pending` - Unscheduled pod removed

## Event Sequences

### Normal Bind Flow
1. `ADDED node=None phase=Pending`
2. Scheduler binds pod to node
3. `MODIFIED node=<node> phase=Pending` (bind result - ignored)
4. `MODIFIED node=<node> phase=Running` (pod started - ignored)

### Preemption & Recreation Flow
1. `DELETED node=<node> phase=Pending/Succeeded` (preempted pod deleted)
2. `ADDED node=None phase=Pending` (controller recreates with **NEW UID**)
3. Scheduler binds new pod instance

**Key insight**: Pod id changes changes across delete→recreate cycles.

## Gang Scheduling Considerations

When gang members are preempted:
- Track by **name** (persists) not **UID** (changes on recreation)
- Gang reformation only complete when all expected pod **names** return
- Prevents premature gang completion and scheduling loops
