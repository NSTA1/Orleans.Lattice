-- Per-partition projection checkpoints, per tree.
--
-- Emits one pipe-delimited row per (tree, partition):
--   <treeId>|<partition>|<leaves>|<distinctCheckpoints>|<minOffset>|<maxOffset>
--
-- LeafNodeState.ProjectionCheckpointOffsetsByPartition is the durable record
-- of how far each leaf's projection has been flushed for each WAL partition.
-- A tree whose partition shows exactly ONE distinct checkpoint value across
-- every leaf has made no independent durable progress on that partition,
-- which is the signature the epic attributes to the deferred-mutation clamp
-- (S1). A healthy partition shows several distinct values.
.mode list
.headers off

WITH leaf AS (
    SELECT CAST(PayloadBinary AS TEXT) AS doc
    FROM OrleansStorage
    WHERE GrainTypeString = 'leaf'
      AND PayloadBinary IS NOT NULL
),
extracted AS (
    SELECT
        json_extract(doc, '$.TreeId') AS treeId,
        json_extract(doc, '$.ProjectionCheckpointOffsetsByPartition."$values"') AS parts
    FROM leaf
)
SELECT
    extracted.treeId
    || '|' || part.key
    || '|' || COUNT(*)
    || '|' || COUNT(DISTINCT part.value)
    || '|' || MIN(part.value)
    || '|' || MAX(part.value)
FROM extracted, json_each(extracted.parts) AS part
WHERE extracted.parts IS NOT NULL
  AND extracted.treeId IS NOT NULL
GROUP BY extracted.treeId, part.key
ORDER BY extracted.treeId, part.key;
