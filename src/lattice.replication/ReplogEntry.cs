using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// A single change-feed record captured at commit time by the replication
/// package. Authored synchronously inside the originating grain's write
/// path (via the core <see cref="IMutationObserver"/> hook) and forwarded
/// to the registered <c>IReplogSink</c> before the grain method returns,
/// so the captured value is guaranteed to be the value that was just
/// committed - replication consumers never need to re-read the primary.
/// <para>
/// The shape is deliberately flat: every field necessary to apply the
/// mutation on a remote cluster (op, key, range bound, value bytes,
/// hybrid-logical clock, tombstone flag, expiry, origin cluster id) is a
/// top-level <c>[Id]</c> slot so the record round-trips through the
/// Orleans serializer without depending on any internal core DTO.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplogEntry)]
[Immutable]
public readonly record struct ReplogEntry
{
    /// <summary>The logical tree id the mutation was committed to.</summary>
    [Id(0)] public string TreeId { get; init; }

    /// <summary>The kind of mutation.</summary>
    [Id(1)] public ReplogOp Op { get; init; }

    /// <summary>
    /// The key for <see cref="ReplogOp.Set"/> / <see cref="ReplogOp.Delete"/>,
    /// or the inclusive start key for <see cref="ReplogOp.DeleteRange"/>.
    /// </summary>
    [Id(2)] public string Key { get; init; }

    /// <summary>
    /// The exclusive end key for <see cref="ReplogOp.DeleteRange"/>;
    /// <c>null</c> for <see cref="ReplogOp.Set"/> and <see cref="ReplogOp.Delete"/>.
    /// </summary>
    [Id(3)] public string? EndExclusiveKey { get; init; }

    /// <summary>
    /// The committed value for <see cref="ReplogOp.Set"/>; <c>null</c>
    /// for deletes and range deletes.
    /// </summary>
    [Id(4)] public byte[]? Value { get; init; }

    /// <summary>
    /// The <see cref="HybridLogicalClock"/> stamped on the committed entry
    /// for <see cref="ReplogOp.Set"/> and <see cref="ReplogOp.Delete"/>.
    /// For <see cref="ReplogOp.DeleteRange"/> this carries
    /// <see cref="HybridLogicalClock.Zero"/> because a single range may
    /// produce many per-leaf HLCs that cannot be faithfully collapsed.
    /// </summary>
    [Id(5)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// <c>true</c> when the committed entry is a tombstone
    /// (<see cref="ReplogOp.Delete"/> and <see cref="ReplogOp.DeleteRange"/>
    /// always set this).
    /// </summary>
    [Id(6)] public bool IsTombstone { get; init; }

    /// <summary>
    /// Absolute UTC tick at which the committed entry expires, or <c>0</c>
    /// when it does not expire. Preserved end-to-end for
    /// <see cref="ReplogOp.Set"/>; always <c>0</c> for deletes.
    /// </summary>
    [Id(7)] public long ExpiresAtTicks { get; init; }

    /// <summary>
    /// Identifier of the cluster that authored this mutation. Stamped at
    /// commit time from either the mutation's pre-existing
    /// <see cref="LatticeMutation.OriginClusterId"/> (for replays of a
    /// remote write) or the validated local
    /// <see cref="LatticeReplicationOptions.ClusterId"/> for local-origin
    /// writes. Receivers use this to attribute origin and break replication
    /// cycles. Will be non-empty for every entry produced by the standard
    /// commit-time observer because the options validator rejects an
    /// empty <c>ClusterId</c> at first-resolve time; <c>null</c> only on
    /// hand-constructed entries used in tests.
    /// </summary>
    [Id(8)] public string? OriginClusterId { get; init; }
}

