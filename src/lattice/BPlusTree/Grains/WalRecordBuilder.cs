using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal helper that constructs a <see cref="WalRecord"/> directly
/// from the leaf-side commit context. Replaces the historical
/// <c>WalRecordConverter.ToWalRecord(LatticeMutation, ...)</c> hop that
/// every leaf write previously paid; the WAL append path now allocates
/// a single per-entry record directly at the producer site.
/// <para>
/// The producer-side <see cref="WalRecord.Mode"/> stamp and the
/// fallback <see cref="WalRecord.OriginClusterId"/> resolution remain
/// the responsibility of <see cref="WalCommitLogWriter"/> because both
/// are uniform across every leaf in a tree and live behind the
/// <see cref="ILatticeMergeModeResolver"/> /
/// <see cref="ILatticeOriginClusterIdResolver"/> seams the leaf does
/// not consume directly.
/// </para>
/// </summary>
internal static class WalRecordBuilder
{
    /// <summary>
    /// Builds a <see cref="WalRecord"/> for a per-key foreground
    /// <see cref="MutationKind.Set"/> commit. The caller supplies the
    /// already-stamped LWW entry; the per-leaf commit context
    /// (transaction id, atomic-batch position, ambient delta and
    /// maintenance flag) is captured from the standard
    /// <c>Lattice*Context</c> ambients so the per-call argument list
    /// stays small.
    /// </summary>
    public static WalRecord ForSet(
        string treeId,
        int shardIndex,
        string key,
        LwwValue<byte[]> committed,
        bool isPrepared)
    {
        var delta = LatticeDeltaContext.Current;
        var batch = LatticeAtomicBatchContext.Current;
        return new WalRecord
        {
            TreeId = treeId,
            Op = MutationKind.Set,
            Key = key,
            Value = committed.IsTombstone ? null : committed.Value,
            Timestamp = committed.Timestamp,
            IsTombstone = committed.IsTombstone,
            ExpiresAtTicks = committed.ExpiresAtTicks,
            OriginClusterId = committed.OriginClusterId,
            VectorClock = committed.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            Delta = delta,
            AtomicBatchSize = batch?.Size ?? 0,
            AtomicBatchIndex = batch?.Index ?? 0,
            IsPrepared = isPrepared,
            ShardIndex = shardIndex,
        };
    }

    /// <summary>
    /// Builds a <see cref="WalRecord"/> for a per-key foreground
    /// <see cref="MutationKind.Delete"/> commit. The supplied tombstone
    /// already carries the producer-stamped HLC, origin, and vector
    /// clock; this helper only mirrors them onto the WAL record shape.
    /// </summary>
    public static WalRecord ForDelete(
        string treeId,
        int shardIndex,
        string key,
        LwwValue<byte[]> tombstone,
        bool isPrepared)
    {
        var delta = LatticeDeltaContext.Current;
        var batch = LatticeAtomicBatchContext.Current;
        return new WalRecord
        {
            TreeId = treeId,
            Op = MutationKind.Delete,
            Key = key,
            Timestamp = tombstone.Timestamp,
            IsTombstone = true,
            OriginClusterId = tombstone.OriginClusterId,
            VectorClock = tombstone.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            Delta = delta,
            AtomicBatchSize = batch?.Size ?? 0,
            AtomicBatchIndex = batch?.Index ?? 0,
            IsPrepared = isPrepared,
            ShardIndex = shardIndex,
        };
    }

    /// <summary>
    /// Builds a <see cref="WalRecord"/> for a foreground
    /// <see cref="MutationKind.DeleteRange"/> commit. The leaf-side
    /// caller has already produced the producer-issue tombstone
    /// (<paramref name="tombstone"/>) carrying the pinned issue HLC,
    /// origin, and vector clock; the WAL record carries the same
    /// triple with the inclusive-start / exclusive-end pair the range
    /// covers.
    /// </summary>
    public static WalRecord ForDeleteRange(
        string treeId,
        int shardIndex,
        string startInclusive,
        string endExclusive,
        LwwValue<byte[]> tombstone,
        IReadOnlyList<string>? matchedKeys = null)
    {
        var delta = LatticeDeltaContext.Current;
        return new WalRecord
        {
            TreeId = treeId,
            Op = MutationKind.DeleteRange,
            Key = startInclusive,
            EndExclusiveKey = endExclusive,
            Timestamp = tombstone.Timestamp,
            IsTombstone = true,
            OriginClusterId = tombstone.OriginClusterId,
            VectorClock = tombstone.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            Delta = delta,
            ShardIndex = shardIndex,
            MatchedKeys = matchedKeys,
        };
    }

    /// <summary>
    /// Builds a <see cref="WalRecord"/> for a producer-side typed CRDT
    /// delta-apply commit. Unlike <see cref="ForSet"/>, the delta-apply
    /// path carries the typed delta bytes in <see cref="WalRecord.Delta"/>
    /// and leaves <see cref="WalRecord.Value"/> <see langword="null"/>:
    /// the post-merge full state is never materialised into the WAL
    /// record. The canonical encoder already strips
    /// <see cref="WalRecord.Value"/> for non-prepared CRDT-mode Set
    /// records, so the persisted/wire bytes are unchanged; leaving the
    /// in-memory slot null as well means the durable writer path no
    /// longer pays an O(state) post-merge serialisation solely to feed a
    /// slot that is dropped on encode. Receivers (and the cold-rebuild
    /// replay path) reconstruct the post-fold state by folding
    /// <see cref="WalRecord.Delta"/> into their current visible state via
    /// the registered <c>CrdtShape</c>. The merge mode is stamped onto
    /// the in-memory record so the leaf-side dispatch can dispatch on
    /// <paramref name="mode"/> without re-resolving it; the wire-time
    /// encoder re-stamps from the per-tree resolver.
    /// </summary>
    public static WalRecord ForCrdtDelta(
        string treeId,
        int shardIndex,
        string key,
        LatticeMergeMode mode,
        LwwValue<byte[]> postMerge,
        byte[] deltaBytes)
    {
        var batch = LatticeAtomicBatchContext.Current;
        return new WalRecord
        {
            TreeId = treeId,
            Op = MutationKind.Set,
            Key = key,
            // Delta-only on the wire: the post-merge state row is never
            // carried here. The encoder strips Value for this record
            // shape regardless, and the replay/receiver fold reconstructs
            // the state from Delta. See the method docstring.
            Value = null,
            Timestamp = postMerge.Timestamp,
            IsTombstone = false,
            ExpiresAtTicks = postMerge.ExpiresAtTicks,
            OriginClusterId = postMerge.OriginClusterId,
            VectorClock = postMerge.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            Delta = deltaBytes,
            Mode = mode,
            AtomicBatchSize = batch?.Size ?? 0,
            AtomicBatchIndex = batch?.Index ?? 0,
            IsPrepared = false,
            ShardIndex = shardIndex,
        };
    }
}