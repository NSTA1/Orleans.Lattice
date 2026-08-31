-- Healing bootstrap census.
--
-- Emits pipe-delimited rows in two sections:
--   grain|<GrainTypeString>|<rows>
--   reminder|<ReminderName>|<rows>
--
-- WHY THIS EXISTS. Shard healing (S11 / #1841) is driven by a per-tree
-- ShardHealingOrchestratorGrain that is bootstrapped LAZILY, from
-- LatticeGrain.EnsureMonitorAsync, which is reached only from WRITE paths. The
-- orchestrator persists its own state and anchors itself with a keepalive
-- reminder, so both of those are durable evidence that healing actually armed
-- on this deployment. Their ABSENCE is the durable evidence that it never did -
-- which is exactly what a converged, read-only deployment produces, and is
-- invisible from the outside because a tree that is never swept logs nothing.
--
-- Read offline against a copy of the volume, with the stack down, so the answer
-- is a property of what the box persisted rather than of what it happened to be
-- doing when it was asked.
.mode list
.headers off

SELECT 'grain|' || GrainTypeString || '|' || COUNT(*)
FROM OrleansStorage
GROUP BY GrainTypeString
ORDER BY GrainTypeString;

SELECT 'reminder|' || ReminderName || '|' || COUNT(*)
FROM OrleansRemindersTable
GROUP BY ReminderName
ORDER BY ReminderName;
