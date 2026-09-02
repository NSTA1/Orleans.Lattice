-- Grain-state size by grain type.
--
-- Emits one pipe-delimited row per Orleans grain type:
--   <grainType>|<rows>|<payloadBytes>
--
-- This is the top-level shape of the durable store: it is what shows that
-- `leaf-snapshot` dominates the SQLite file, and it is the figure a snapshot
-- codec change (S3) moves.
.mode list
.headers off

SELECT
    GrainTypeString
    || '|' || COUNT(*)
    || '|' || COALESCE(SUM(LENGTH(PayloadBinary)), 0)
FROM OrleansStorage
GROUP BY GrainTypeString
ORDER BY COUNT(*) DESC;
