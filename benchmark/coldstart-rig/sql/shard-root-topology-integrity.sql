-- Shard-root topology integrity census.
--
-- Emits one pipe-delimited row per tree, then a TOTAL row:
--   <treeId>|<okLeaf>|<okInternal>|<flagLeafOverInternal>|<flagInternalOverLeaf>|<rootIdMissing>
--
-- WHAT IT CHECKS. A shard root persists two facts about its root node that
-- must agree: `RootNodeId`, which names the grain, and `RootIsLeaf`, a bool.
-- They can disagree on disk. `ShardRootGrain.Traversal.cs` calls that state "a
-- baked-inconsistent topology that left the RootIsLeaf bit true over an
-- internal root (issue 899)", and records that the consequence of acting on
-- the flag alone is "blind-casting the internal root to IBPlusLeafGrain" -
-- an InvalidCastException: BPlusInternalGrain -> IBPlusLeafGrain.
--
-- WHY IT IS WORTH A COMMITTED QUERY. This is not a synthetic-load artifact.
-- Run against the pristine 2026-08-29 backup of the live RepoContext
-- deployment, it reported 96 of 841 shard roots (about 11 percent) in the
-- flag-leaf-over-internal state, in a backup predating epic #1830 entirely. So
-- any traversal path that reads `state.State.RootIsLeaf` raw - rather than
-- `RootIsLeafTyped`, or a raw read followed by an `IsLeafGrainId` check - is
-- exposed on roughly one shard root in nine of a real deployment.
--
-- THE PER-TREE BREAKDOWN IS THE ACTIONABLE PART, and on that volume it is not
-- diffuse at all - it is confined entirely to the vector trees:
--
--   repo-context-vector-metadata      0 ok, 64 flag-leaf-over-internal  (100%)
--   repo-context-vector-membership   32 ok, 32 flag-leaf-over-internal   (50%)
--   repo-context-vector-payload      64 ok (correctly flagged internal)   (0%)
--   every other tree                 64 ok leaf                           (0%)
--
-- Note what that rules out. Being internal-rooted does not itself produce the
-- condition: `vector-payload` is internal-rooted on all 64 shards and every one
-- is flagged correctly. The trees that carry it are the ones that mutate in
-- place - epic #1830 recorded `vector-metadata` as the tree issuing
-- `DeleteRangeAsync` and `vector-membership` as plain `Set`/`OrFlag`, while
-- `vector-payload` is write-once. So the correlation is with in-place mutation
-- and range deletion, not with tree size or root depth. That is a lead across
-- three trees, not a proof - but it is where to look first.
--
-- A single total would have hidden all of this, which is why the query groups.
--
-- Read offline, with the stack down, so the answer is a property of what is on
-- disk rather than of what the box happened to be doing. Needs no cluster and
-- no cold start, which makes it the cheapest integrity check in this rig. Point
-- it at a copy of any volume - `snapshot-volume.ps1` extracts one.
--
-- The shard-root grain key is "{treeId}/{shardIndex}" and a tree id never
-- contains a slash, so the tree is everything before the first separator.
.mode list
.headers off

WITH roots AS (
    SELECT
        GrainIdExtensionString AS grainKey,
        CAST(PayloadBinary AS TEXT) AS doc
    FROM OrleansStorage
    WHERE GrainTypeString = 'shardroot'
      AND PayloadBinary IS NOT NULL
      AND GrainIdExtensionString IS NOT NULL
      AND instr(GrainIdExtensionString, '/') > 0
),
classified AS (
    SELECT
        substr(grainKey, 1, instr(grainKey, '/') - 1) AS treeId,
        CASE WHEN instr(doc, '"RootIsLeaf":true') > 0 THEN 1 ELSE 0 END AS flagLeaf,
        CASE WHEN instr(doc, '"RootNodeId":"bplusleaf/') > 0 THEN 1 ELSE 0 END AS idLeaf,
        CASE WHEN instr(doc, '"RootNodeId":"') = 0 THEN 1 ELSE 0 END AS idMissing
    FROM roots
),
tallied AS (
    SELECT
        treeId,
        SUM(CASE WHEN idMissing = 0 AND flagLeaf = 1 AND idLeaf = 1 THEN 1 ELSE 0 END) AS okLeaf,
        SUM(CASE WHEN idMissing = 0 AND flagLeaf = 0 AND idLeaf = 0 THEN 1 ELSE 0 END) AS okInternal,
        SUM(CASE WHEN idMissing = 0 AND flagLeaf = 1 AND idLeaf = 0 THEN 1 ELSE 0 END) AS flagLeafOverInternal,
        SUM(CASE WHEN idMissing = 0 AND flagLeaf = 0 AND idLeaf = 1 THEN 1 ELSE 0 END) AS flagInternalOverLeaf,
        SUM(idMissing) AS rootIdMissing
    FROM classified
    GROUP BY treeId
)
SELECT treeId || '|' || okLeaf || '|' || okInternal || '|'
       || flagLeafOverInternal || '|' || flagInternalOverLeaf || '|' || rootIdMissing
FROM tallied
ORDER BY flagLeafOverInternal DESC, flagInternalOverLeaf DESC, treeId;

WITH roots AS (
    SELECT CAST(PayloadBinary AS TEXT) AS doc
    FROM OrleansStorage
    WHERE GrainTypeString = 'shardroot'
      AND PayloadBinary IS NOT NULL
      AND GrainIdExtensionString IS NOT NULL
      AND instr(GrainIdExtensionString, '/') > 0
),
classified AS (
    SELECT
        CASE WHEN instr(doc, '"RootIsLeaf":true') > 0 THEN 1 ELSE 0 END AS flagLeaf,
        CASE WHEN instr(doc, '"RootNodeId":"bplusleaf/') > 0 THEN 1 ELSE 0 END AS idLeaf,
        CASE WHEN instr(doc, '"RootNodeId":"') = 0 THEN 1 ELSE 0 END AS idMissing
    FROM roots
)
SELECT 'TOTAL|'
       || SUM(CASE WHEN idMissing = 0 AND flagLeaf = 1 AND idLeaf = 1 THEN 1 ELSE 0 END) || '|'
       || SUM(CASE WHEN idMissing = 0 AND flagLeaf = 0 AND idLeaf = 0 THEN 1 ELSE 0 END) || '|'
       || SUM(CASE WHEN idMissing = 0 AND flagLeaf = 1 AND idLeaf = 0 THEN 1 ELSE 0 END) || '|'
       || SUM(CASE WHEN idMissing = 0 AND flagLeaf = 0 AND idLeaf = 1 THEN 1 ELSE 0 END) || '|'
       || SUM(idMissing)
FROM classified;
