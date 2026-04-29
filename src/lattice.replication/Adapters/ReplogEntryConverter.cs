using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Adapters;

/// <summary>
/// Static helper translating between the public
/// <see cref="LatticeMutation"/> shape (the core library''s
/// observer-payload contract) and the replication package''s wire-shaped
/// <see cref="ReplogEntry"/>. Used by the the dormant seam commit-log adapters
/// (<see cref="ReplicationCommitLogWriter"/>,
/// <see cref="ReplicationCommitLogReader"/>) so the core library can
/// drive WAL append / read through internal seams without depending on
/// the replication wire DTOs.
/// </summary>
/// <remarks>
/// Translation mirrors the field-by-field semantics already implemented
/// by <c>ReplicationMutationObserver.OnMutationAsync</c>: every flat
/// <c>[Id]</c> slot on <see cref="LatticeMutation"/> has a matching slot
/// on <see cref="ReplogEntry"/> (and vice versa for the reverse
/// direction). The producer-side translation defensively snapshots the
/// mutable <see cref="VersionVector"/> reference to detach the captured
/// entry from any post-emit advance of the producer-side frontier; the
/// reverse translation does not need to clone because
/// <see cref="ReplogEntry"/> is an immutable record carrying the
/// already-detached frontier.
/// </remarks>
internal static class ReplogEntryConverter
{
    /// <summary>
    /// Translates <paramref name="mutation"/> into a
    /// <see cref="ReplogEntry"/> stamped with <paramref name="mode"/>
    /// and <paramref name="originClusterId"/>. The supplied
    /// <paramref name="originClusterId"/> is used only when the
    /// mutation does not already carry one (the mutation''s origin wins
    /// when present, mirroring the "preserve a remote replay''s origin"
    /// semantics of the commit-time observer).
    /// </summary>
    public static ReplogEntry ToReplogEntry(
        LatticeMutation mutation,
        ReplicationMode mode,
        string originClusterId)
    {
        ArgumentNullException.ThrowIfNull(originClusterId);

        var op = mutation.Kind switch
        {
            MutationKind.Set => ReplogOp.Set,
            MutationKind.Delete => ReplogOp.Delete,
            MutationKind.DeleteRange => ReplogOp.DeleteRange,
            _ => throw new InvalidOperationException(
                $"Unknown mutation kind: {mutation.Kind}"),
        };

        // Defensive snapshot of the producer-side frontier (matches the
        // ReplicationMutationObserver convention).
        var capturedFrontier = mutation.VectorClock?.Clone();

        return new ReplogEntry
        {
            TreeId = mutation.TreeId,
            Op = op,
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
            DeltaKind = mutation.DeltaKind,
            DeltaPayload = mutation.DeltaPayload,
        };
    }

    /// <summary>
    /// Translates a <see cref="ReplogEntry"/> back into the public
    /// <see cref="LatticeMutation"/> shape. The reverse direction is
    /// strictly metadata-preserving: every field on
    /// <see cref="ReplogEntry"/> that has a matching slot on
    /// <see cref="LatticeMutation"/> round-trips verbatim. The
    /// translation does not introduce a fresh
    /// <see cref="MutationCategory"/> or <see cref="LatticeMutation.TransactionId"/>
    /// — both default to their wire-compatible defaults
    /// (<see cref="MutationCategory.User"/> and <see cref="System.Guid.Empty"/>)
    /// because the replication wire format does not carry them today.
    /// A future replication WAL extension that carries them will widen
    /// this translation in the same change.
    /// </summary>
    public static LatticeMutation FromReplogEntry(in ReplogEntry entry)
    {
        var kind = entry.Op switch
        {
            ReplogOp.Set => MutationKind.Set,
            ReplogOp.Delete => MutationKind.Delete,
            ReplogOp.DeleteRange => MutationKind.DeleteRange,
            _ => throw new InvalidOperationException(
                $"Unknown replog op: {entry.Op}"),
        };

        return new LatticeMutation
        {
            TreeId = entry.TreeId ?? string.Empty,
            Kind = kind,
            Key = entry.Key ?? string.Empty,
            EndExclusiveKey = entry.EndExclusiveKey,
            Value = entry.Value,
            Timestamp = entry.Timestamp,
            IsTombstone = entry.IsTombstone,
            ExpiresAtTicks = entry.ExpiresAtTicks,
            OriginClusterId = entry.OriginClusterId,
            VectorClock = entry.VectorClock,
            // TransactionId and Category are not on the replication wire
            // today; leave at wire-compat defaults.
            TransactionId = Guid.Empty,
            Category = MutationCategory.User,
            DeltaKind = entry.DeltaKind,
            DeltaPayload = entry.DeltaPayload,
        };
    }
}
