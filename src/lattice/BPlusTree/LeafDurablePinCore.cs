namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Verified core for the durable materialiser pin a leaf publishes for one WAL
/// partition: the coverage-gated trim entitlement the shared-shard WAL GC folds
/// into its offset floor. <c>BPlusLeafGrain.ResolveDurablePinForPartition</c>
/// gathers the inputs and maps the verdict onto a frontier and an offset, so the
/// production publishers and the end-to-end WAL durability Coyote model
/// (<c>WalDurabilityLifecycleModel</c>) execute the identical rule.
/// <para>
/// The pin store merges every published offset by monotonic maximum and can
/// never take one back, so this rule is the whole of the leaf's say over how far
/// the GC may trim. Its invariants, each pinned by <c>LeafDurablePinCoreTests</c>:
/// a pin that authorises trimming never exceeds the PERSISTED checkpoint (issue
/// #3476) nor, for a leaf that has applied a write, the durable snapshot coverage;
/// and a partition whose committed prefix has no durable copy but the WAL keeps
/// the Zero block pin.
/// </para>
/// </summary>
internal static class LeafDurablePinCore
{
    /// <summary>
    /// Resolves the durable pin for one partition.
    /// </summary>
    /// <param name="currentCheckpoint">
    /// The partition's current checkpoint - the higher of the persisted and the
    /// pending one (<c>GetCurrentCheckpointForPartition</c>). Read only to decide
    /// whether the partition has applied anything at all; it never reaches a
    /// published trim offset except as the <c>-1</c> sentinel of an empty release.
    /// </param>
    /// <param name="persistedCheckpoint">
    /// The partition's persisted checkpoint (<c>GetPersistedCheckpointForPartition</c>).
    /// </param>
    /// <param name="coveredOffset">
    /// The durable snapshot coverage this activation has recorded for the
    /// partition (<c>DurableSnapshotCoverageForPartition</c>), <c>-1</c> when none.
    /// </param>
    /// <param name="hasLiveData">Whether any live cache row routes to the partition.</param>
    /// <param name="walProvenEmpty">
    /// Whether the partition's WAL is proven empty (head 0); <see langword="false"/>
    /// when unknown, which keeps the block.
    /// </param>
    /// <param name="releaseNeverWrittenScannedThrough">
    /// Whether the caller opts into the never-written release of issue #3453 AND
    /// the leaf's clock is still Zero. Only the flush paths opt in.
    /// </param>
    /// <returns>The pin verdict.</returns>
    public static LeafDurablePinDecision Resolve(
        long currentCheckpoint,
        long persistedCheckpoint,
        long coveredOffset,
        bool hasLiveData,
        bool walProvenEmpty,
        bool releaseNeverWrittenScannedThrough)
    {
        // Genuinely empty partition: it has applied NOTHING (checkpoint < 0) AND
        // holds no live cache row, so there is no committed prefix to lose.
        // Release any block and report the real frontier (#1490's narrowness).
        //
        // The checkpoint clause is load-bearing. Emptiness is read from the
        // transient per-activation cache, which does not reflect durable data
        // before the cache is hydrated (a cold reactivation with no snapshot, or
        // after tombstone reaping / compaction). A partition with a durable
        // checkpoint must be coverage-gated however empty the cache looks, or the
        // GC may trim a checkpointed, un-snapshotted prefix and the next cold
        // rebuild falls off the log (LeafProjectionStaleException).
        if (!hasLiveData && currentCheckpoint < 0)
        {
            return new LeafDurablePinDecision(LeafDurablePinKind.ReleaseEmpty, currentCheckpoint);
        }

        // Issue #3103: data-bearing, never checkpointed, and its WAL is EMPTY (no
        // entry was ever appended - a WAL reset preserved the snapshot the rows
        // came from). There is nothing to replay, so the checkpoint can never
        // leave -1 and a block would be permanent and tree-wide. Released on a
        // PROVEN-empty WAL only; an unreadable head keeps the block.
        if (currentCheckpoint < 0 && walProvenEmpty)
        {
            return new LeafDurablePinDecision(LeafDurablePinKind.ReleaseEmpty, currentCheckpoint);
        }

        // Issue #3453: a never-written leaf (Zero clock) that has scanned this
        // partition through a PERSISTED checkpoint X >= 0 over other leaves'
        // entries, and holds no live row here. Every release above collapses to
        // (Zero, -1) - the block pin itself - for such a leaf, so (Zero, X) is the
        // only release its encoding can express. X is the persisted checkpoint,
        // never the current one: an over-report can never be withdrawn and every
        // replay starts from the persisted offset (#3476).
        //
        // Not bounded by coverageOffset. A never-written leaf can hold an empty
        // snapshot below X; trimming past that snapshot's coverage makes the next
        // activation rehydrate it and latch stale over a prefix it never owned
        // (issue #4456). Kept verbatim here so the extraction is
        // behaviour-preserving; the WAL durability model records the gap.
        if (releaseNeverWrittenScannedThrough && !hasLiveData && persistedCheckpoint >= 0)
        {
            return new LeafDurablePinDecision(LeafDurablePinKind.ReleaseNeverWritten, persistedCheckpoint);
        }

        // Data-bearing partition, or a durably-checkpointed partition whose cache
        // is momentarily empty. Its checkpointed prefix's only durable copy -
        // absent a snapshot - is the WAL, so authorise trimming only as far as a
        // durable snapshot covers, and never past the PERSISTED checkpoint
        // (#3476): a pending advance lives only in this activation's memory, while
        // every replay starts from the persisted offset.
        var safeOffset = Math.Min(persistedCheckpoint, coveredOffset);
        return safeOffset < 0
            ? new LeafDurablePinDecision(LeafDurablePinKind.Block, -1L)
            : new LeafDurablePinDecision(LeafDurablePinKind.Covered, safeOffset);
    }
}
