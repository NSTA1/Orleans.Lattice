-- Shard-root topology integrity census.
--
-- Emits one pipe-delimited row per tree, then a TOTAL row:
--   <treeId>|<okLeaf>|<okInternal>|<flagLeafOverInternal>|<flagInternalOverLeaf>|<flagAbsentOverInternal>|<rootIdMissing>
--
-- The `flagAbsentOverInternal` column is the LATENT bucket: an internal root
-- whose blob carries no `RootIsLeaf` field at all. It is counted separately
-- from `okInternal` (an explicit `"RootIsLeaf":false`) because before issue
-- #1886 those two on-disk shapes loaded to OPPOSITE values - see below.
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
-- deployment, it reported 96 of 841 shard roots carrying the
-- flag-leaf-over-internal state ON DISK, in a backup predating epic #1830
-- entirely.
--
-- *** 96 IS THE BLOB COUNT, NOT THE EXPOSURE. READ THIS BEFORE QUOTING IT. ***
-- This query reads the PERSISTED BLOB; the runtime reads the DESERIALIZED
-- OBJECT, and before issue #1886 they differed. `ShardRootState.RootIsLeaf` was
-- declared `= true`, and the storage serializer omits `default(bool)`, so a
-- correctly-written `false` was dropped on save and resurrected as `true` on
-- load. A further 64 shard roots therefore had NO `RootIsLeaf` FIELD AT ALL and
-- manufactured the same lie at load time. The pre-#1886 runtime exposure was
-- 160 of 160 internal-rooted shard roots - every single one - of which 96 had
-- it baked into the blob and 64 were latent. "One shard root in nine" was an
-- undercount of exactly those 64.
--
-- THE PER-TREE BREAKDOWN IS THE ACTIONABLE PART, and it separates BAKED from
-- LATENT rather than corrupt from clean:
--
--   tree                             on disk                    at runtime (pre-#1886)
--   repo-context-vector-metadata     64 flag-leaf-over-internal  64 wrong  (baked)
--   repo-context-vector-membership   32 flag-leaf-over-internal  32 wrong  (baked)
--   repo-context-vector-payload      64 with NO RootIsLeaf field 64 wrong  (latent)
--   every other tree                 64 ok leaf                   ok
--
-- SO PAYLOAD WAS NEVER A CLEAN CONTROL. An earlier revision of this header read
-- its 64 rows as "correctly flagged internal" and built an argument on the
-- contrast; that was an artefact of reading the blob and calling it the state.
-- The corruption is UNIVERSAL across internal-rooted roots. What differs by tree
-- is only whether it is BAKED or LATENT, and that is decided by RE-SAVE
-- frequency:
--
--   1. promotion correctly writes RootIsLeaf = false
--   2. the serializer omits it (false IS default(bool))
--   3. reload yields true from the property initializer - the lie, still latent
--   4. any LATER state write persists "RootIsLeaf":true LITERALLY, because true
--      is no longer the default - now baked into the blob
--
-- `vector-metadata` (DeleteRangeAsync) and `vector-membership` (Set / OrFlag)
-- mutate in place, so they reached step 4 and baked. `vector-payload` is
-- write-once, so it never re-saved after a reload and its blob stayed clean.
-- In-place mutation CONVERTS a latent corruption into a persisted one; it does
-- not cause it. A single total would have hidden all of this, which is why the
-- query groups by tree.
--
-- *** WHAT THE NUMBERS MEAN NOW, POST-#1886 AND #1883. *** Both have landed, so
-- this query's output on an old volume is a BEFORE reading. Do not diagnose a
-- healthy box from it.
--   * #1886 removed the `= true` initializers from `ShardRootState.RootIsLeaf`
--     and `InternalNodeState.ChildrenAreLeaves`. An absent field now reads
--     `false`, which is exactly what was written - so the 64 LATENT rows are
--     correct on load and need no repair at all.
--   * #1883 added an activation-time repair
--     (`ShardRootGrain.RootFlagHeal.cs`). The 96 BAKED rows are what it drains,
--     logging "repaired an inconsistent persisted RootIsLeaf flag (issue 899 /
--     issue 1883)" as each shard activates.
-- That is why this query is still worth committing after the fix: it is the
-- instrument that distinguishes the roots needing the heal from the ones the
-- POCO change repairs on load, and it is how you confirm the heal has drained a
-- volume rather than assuming it.
--
-- A CAVEAT ON THIS QUERY'S OWN READING, for whoever extends it. Testing only
-- `instr(doc,'"X":true')` answers "what is stored", not "what will load". That
-- is why the ABSENT case is a column of its own (`flagAbsentOverInternal`) and
-- is never folded into `okInternal`: an earlier revision of this query counted
-- absent as "correctly flagged internal", which is what let payload look like a
-- clean control tree. Always decide what a POCO's initializer will make of an
-- omitted field before reporting a field's value as the state.
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
        -- The ABSENT case is its own bucket, never folded into "false". Pre-#1886
        -- an absent field loaded as TRUE (the `= true` initializer over an
        -- omitted default), so absent and explicit-false were opposite states on
        -- load despite both being "not true" on disk.
        CASE WHEN instr(doc, '"RootIsLeaf"') = 0 THEN 1 ELSE 0 END AS flagAbsent,
        CASE WHEN instr(doc, '"RootNodeId":"bplusleaf/') > 0 THEN 1 ELSE 0 END AS idLeaf,
        CASE WHEN instr(doc, '"RootNodeId":"') = 0 THEN 1 ELSE 0 END AS idMissing
    FROM roots
),
tallied AS (
    SELECT
        treeId,
        SUM(CASE WHEN idMissing = 0 AND flagLeaf = 1 AND idLeaf = 1 THEN 1 ELSE 0 END) AS okLeaf,
        SUM(CASE WHEN idMissing = 0 AND flagLeaf = 0 AND flagAbsent = 0 AND idLeaf = 0 THEN 1 ELSE 0 END) AS okInternal,
        SUM(CASE WHEN idMissing = 0 AND flagLeaf = 1 AND idLeaf = 0 THEN 1 ELSE 0 END) AS flagLeafOverInternal,
        SUM(CASE WHEN idMissing = 0 AND flagLeaf = 0 AND flagAbsent = 0 AND idLeaf = 1 THEN 1 ELSE 0 END) AS flagInternalOverLeaf,
        SUM(CASE WHEN idMissing = 0 AND flagAbsent = 1 AND idLeaf = 0 THEN 1 ELSE 0 END) AS flagAbsentOverInternal,
        SUM(idMissing) AS rootIdMissing
    FROM classified
    GROUP BY treeId
)
SELECT treeId || '|' || okLeaf || '|' || okInternal || '|'
       || flagLeafOverInternal || '|' || flagInternalOverLeaf || '|'
       || flagAbsentOverInternal || '|' || rootIdMissing
FROM tallied
ORDER BY flagLeafOverInternal DESC, flagAbsentOverInternal DESC, flagInternalOverLeaf DESC, treeId;

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
        CASE WHEN instr(doc, '"RootIsLeaf"') = 0 THEN 1 ELSE 0 END AS flagAbsent,
        CASE WHEN instr(doc, '"RootNodeId":"bplusleaf/') > 0 THEN 1 ELSE 0 END AS idLeaf,
        CASE WHEN instr(doc, '"RootNodeId":"') = 0 THEN 1 ELSE 0 END AS idMissing
    FROM roots
)
SELECT 'TOTAL|'
       || SUM(CASE WHEN idMissing = 0 AND flagLeaf = 1 AND idLeaf = 1 THEN 1 ELSE 0 END) || '|'
       || SUM(CASE WHEN idMissing = 0 AND flagLeaf = 0 AND flagAbsent = 0 AND idLeaf = 0 THEN 1 ELSE 0 END) || '|'
       || SUM(CASE WHEN idMissing = 0 AND flagLeaf = 1 AND idLeaf = 0 THEN 1 ELSE 0 END) || '|'
       || SUM(CASE WHEN idMissing = 0 AND flagLeaf = 0 AND flagAbsent = 0 AND idLeaf = 1 THEN 1 ELSE 0 END) || '|'
       || SUM(CASE WHEN idMissing = 0 AND flagAbsent = 1 AND idLeaf = 0 THEN 1 ELSE 0 END) || '|'
       || SUM(idMissing)
FROM classified;
