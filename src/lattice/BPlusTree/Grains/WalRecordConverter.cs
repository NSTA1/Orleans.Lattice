using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Static helper translating between the public
/// <see cref="LatticeMutation"/> shape (the core library's
/// observer-payload contract) and the durability-shaped
/// <see cref="WalRecord"/>. Used by the commit-log adapters
/// (<see cref="WalCommitLogWriter"/>,
/// <see cref="WalCommitLogReader"/>) to drive WAL append / read through
/// internal seams. The two shapes carry the same data; the WAL record
/// adds wire-only fields the producer must stamp at commit time
/// (declared <see cref="LatticeMergeMode"/>, captured causal frontier,
/// <see cref="WalRecord.OriginClusterId"/>) that the public mutation
/// surface does not expose.
/// </summary>
/// <remarks>
/// The producer-side translation defensively snapshots the
/// mutable <see cref="VersionVector"/> reference to detach the captured
/// entry from any post-emit advance of the producer-side frontier; the
/// reverse translation does not need to clone because
/// <see cref="WalRecord"/> is an immutable record carrying the
/// already-detached frontier.
/// </remarks>
internal static class WalRecordConverter
{
    /// <summary>
    /// Translates <paramref name="mutation"/> into a
    /// <see cref="WalRecord"/> stamped with <paramref name="mode"/>
    /// and <paramref name="originClusterId"/>. The supplied
    /// <paramref name="originClusterId"/> is used only when the
    /// mutation does not already carry one (the mutation''s origin wins
    /// when present, mirroring the "preserve a remote replay''s origin"
    /// semantics of the commit-time observer).
    /// </summary>
    public static WalRecord ToWalRecord(
        LatticeMutation mutation,
        LatticeMergeMode mode,
        string originClusterId)
    {
        ArgumentNullException.ThrowIfNull(originClusterId);

        // Defensive snapshot of the producer-side frontier (matches the
        // ReplicationMutationObserver convention).
        var capturedFrontier = mutation.VectorClock?.Clone();

        return new WalRecord
        {
            TreeId = mutation.TreeId,
            Op = mutation.Kind,
            Key = mutation.Key ?? string.Empty,
            EndExclusiveKey = mutation.EndExclusiveKey,
            Value = mutation.Value,
            Timestamp = mutation.Timestamp,
            IsTombstone = mutation.IsTombstone,
            ExpiresAtTicks = mutation.ExpiresAtTicks,
            OriginClusterId = mutation.OriginClusterId ?? originClusterId,
            Mode = mode,
            VectorClock = capturedFrontier,
            DependencySummary = capturedFrontier,
            Delta = mutation.Delta,
            AtomicBatchSize = mutation.AtomicBatchSize,
            AtomicBatchIndex = mutation.AtomicBatchIndex,
            TransactionId = mutation.TransactionId,
            IsPrepared = mutation.IsPrepared,
            ShardIndex = mutation.ShardIndex,
            AtomicShardCount = mutation.AtomicShardCount,
            IsMerge = mutation.IsMerge,
            IsBackstop = mutation.IsBackstop,
            Category = mutation.Category,
            MatchedKeys = mutation.MatchedKeys,
        };
    }

    /// <summary>
    /// Translates a <see cref="WalRecord"/> back into the public
    /// <see cref="LatticeMutation"/> shape. The reverse direction is
    /// strictly metadata-preserving: every field on
    /// <see cref="WalRecord"/> that has a matching slot on
    /// <see cref="LatticeMutation"/> round-trips verbatim, including
    /// the atomic-batch metadata (<see cref="LatticeMutation.TransactionId"/>, 
    /// <see cref="LatticeMutation.AtomicBatchSize"/>, and 
    /// <see cref="LatticeMutation.AtomicBatchIndex"/>) which the
    /// replication wire format carries on every entry. The
    /// translation does not introduce a fresh
    /// <see cref="MutationCategory"/> - it defaults to
    /// <see cref="MutationCategory.User"/> because the replication
    /// wire format does not carry the category today.
    /// </summary>
    public static LatticeMutation FromWalRecord(in WalRecord entry)
    {
        return new LatticeMutation
        {
            TreeId = entry.TreeId ?? string.Empty,
            Kind = entry.Op,
            Key = entry.Key ?? string.Empty,
            EndExclusiveKey = entry.EndExclusiveKey,
            Value = entry.Value,
            Timestamp = entry.Timestamp,
            IsTombstone = entry.IsTombstone,
            ExpiresAtTicks = entry.ExpiresAtTicks,
            OriginClusterId = entry.OriginClusterId,
            VectorClock = entry.VectorClock,
            TransactionId = entry.TransactionId,
            AtomicBatchSize = entry.AtomicBatchSize,
            AtomicBatchIndex = entry.AtomicBatchIndex,
            // Category is round-tripped from the additive WalRecord
            // slot. For legacy entries persisted before that slot
            // existed, the slot defaults to User on decode - matching
            // the historical wire-compat default this method used to
            // apply unconditionally.
            Category = entry.Category,
            Delta = entry.Delta,
            IsPrepared = entry.IsPrepared,
            ShardIndex = entry.ShardIndex,
            AtomicShardCount = entry.AtomicShardCount,
            IsMerge = entry.IsMerge,
            IsBackstop = entry.IsBackstop,
            MatchedKeys = entry.MatchedKeys,
        };
    }
}
