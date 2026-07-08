using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Outcome of an <see cref="IReplicationApplier.ApplyAsync(WalRecord, CancellationToken)"/>
/// invocation. Returned to give callers visibility into whether the
/// entry was actually merged onto the local tree (versus deduped by
/// the per-origin high-water-mark) and where the HWM stands after the
/// call.
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ApplyResult)]
[Immutable]
public readonly record struct ApplyResult
{
    /// <summary>
    /// <c>true</c> when the receiver merged the entry onto the local
    /// tree; <c>false</c> when the entry was filtered out as a
    /// re-delivery (its <see cref="WalRecord.Timestamp"/> was at or
    /// below the per-origin high-water-mark) or rejected as
    /// inapplicable (for example, an entry whose
    /// <see cref="WalRecord.OriginClusterId"/> matches the local
    /// cluster id and would therefore loop locally).
    /// </summary>
    [Id(0)] public bool Applied { get; init; }

    /// <summary>
    /// The per-origin high-water-mark for
    /// <c>(WalRecord.TreeId, WalRecord.OriginClusterId)</c> after
    /// the call returns. When the entry was point-applied (Set / Delete)
    /// this equals the entry's <see cref="WalRecord.Timestamp"/>; when
    /// the entry was filtered out as a re-delivery this is the HWM that
    /// suppressed the apply. For range deletes and local-origin no-op
    /// rejections - neither of which consults the HWM - this is
    /// <see cref="HybridLogicalClock.Zero"/>.
    /// </summary>
    [Id(1)] public HybridLogicalClock HighWaterMark { get; init; }

    /// <summary>
    /// <c>true</c> only when the entry / run was deferred by the durable
    /// inbound receive fence (issue #1173) because a cross-cluster restore
    /// saga has paused inbound apply for this tree. A deferred result is
    /// distinct from every other <see cref="Applied"/><c> == false</c>
    /// outcome (re-delivery dedup, local-origin rejection, tombstone
    /// filtering): those are terminal on the receiver and the sender must
    /// advance its cursor past them, whereas a deferred entry has NOT been
    /// applied and MUST be re-shipped once the fence lifts. Receive paths
    /// translate a deferred result into a not-accepted, cursor-preserving
    /// ack so the sender keeps its per-peer cursor and retries the same
    /// batch after a backoff. Defaults to <c>false</c>, so every existing
    /// result shape (and every construction that omits this member) keeps
    /// its cursor-advancing semantics unchanged.
    /// </summary>
    [Id(2)] public bool Deferred { get; init; }
}
