using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Outcome of an <see cref="IReplicationApplier.ApplyAsync(ReplogEntry, CancellationToken)"/>
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
    /// re-delivery (its <see cref="ReplogEntry.Timestamp"/> was at or
    /// below the per-origin high-water-mark) or rejected as
    /// inapplicable (for example, an entry whose
    /// <see cref="ReplogEntry.OriginClusterId"/> matches the local
    /// cluster id and would therefore loop locally).
    /// </summary>
    [Id(0)] public bool Applied { get; init; }

    /// <summary>
    /// The per-origin high-water-mark for
    /// <c>(ReplogEntry.TreeId, ReplogEntry.OriginClusterId)</c> after
    /// the call returns. When the entry was point-applied (Set / Delete)
    /// this equals the entry's <see cref="ReplogEntry.Timestamp"/>; when
    /// the entry was filtered out as a re-delivery this is the HWM that
    /// suppressed the apply. For range deletes and local-origin no-op
    /// rejections — neither of which consults the HWM — this is
    /// <see cref="HybridLogicalClock.Zero"/>.
    /// </summary>
    [Id(1)] public HybridLogicalClock HighWaterMark { get; init; }
}
