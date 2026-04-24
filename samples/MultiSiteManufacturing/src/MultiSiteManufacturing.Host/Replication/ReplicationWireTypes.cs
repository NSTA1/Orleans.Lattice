using Orleans.Lattice.Primitives;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Well-known identifiers used across the replication subsystem.
/// Kept in one place so tests, the outgoing-call filter, the
/// replicator grain, and the inbound endpoint agree on names.
/// </summary>
/// <remarks>
/// <para>
/// <b>FUTURE seam.</b> When <c>Orleans.Lattice</c> adds a native
/// change-feed / cross-tree continuous-merge capability, the replog
/// tree prefix and the <see cref="ReplayRequestContextKey"/> mechanism
/// become redundant — the library primitive will surface events
/// directly and carry enough provenance to suppress replay cycles on
/// its own. The constants stay here until then so the sample's
/// replication hop can be audited as pure application code.
/// </para>
/// </remarks>
internal static class ReplicationConstants
{
    /// <summary>
    /// Prefix for the per-tree replication log. The full tree id is
    /// <c>{ReplogTreePrefix}{treeName}</c>. Double underscore avoids
    /// collision with any production tree id in the sample.
    /// </summary>
    public const string ReplogTreePrefix = "_replog__";

    /// <summary>
    /// Orleans <see cref="Orleans.Runtime.RequestContext"/> key set by
    /// the inbound replication endpoint on apply; the outgoing call
    /// filter reads this to skip appending to the replog for writes
    /// that are themselves a replicated replay (cycle break
    /// A → B → A). Value is the source cluster name (diagnostic;
    /// flag presence is what matters).
    /// </summary>
    public const string ReplayRequestContextKey = "lattice.replay";

    /// <summary>
    /// HTTP header carrying the shared-secret bearer token used to
    /// authenticate peer-to-peer replication POSTs.
    /// </summary>
    public const string AuthHeader = "X-Replication-Token";

    /// <summary>
    /// HTTP URL path template for inbound batches, with
    /// <c>{tree}</c> as the single route segment.
    /// </summary>
    public const string InboundPath = "/replicate/{tree}";
}

/// <summary>
/// Classifies a replicated write as a forward set or as a tombstone.
/// The payload of a <see cref="Delete"/> entry has no user bytes — the
/// receiver calls <c>DeleteAsync</c> regardless of what
/// <see cref="ReplicationEntry.Value"/> contains.
/// </summary>
public enum ReplicationOp
{
    /// <summary>Forward write — receiver calls <c>SetAsync</c>.</summary>
    Set = 0,

    /// <summary>Tombstone — receiver calls <c>DeleteAsync</c>.</summary>
    Delete = 1,
}

/// <summary>
/// One replicated write, as serialized on the wire between clusters.
/// </summary>
public sealed record ReplicationEntry
{
    /// <summary>The key as stored in the primary lattice tree.</summary>
    public required string Key { get; init; }

    /// <summary>Forward set / delete classification.</summary>
    public required ReplicationOp Op { get; init; }

    /// <summary>
    /// Value bytes at ship time. <c>null</c> for a
    /// <see cref="ReplicationOp.Delete"/> or when the key was deleted
    /// between the replog append and the ship-time primary read.
    /// </summary>
    public byte[]? Value { get; init; }

    /// <summary>
    /// HLC associated with the source write as recorded in the
    /// source cluster's replog. Carried for observability only; the
    /// receiver's local lattice assigns its own HLC on apply.
    /// </summary>
    public required HybridLogicalClock SourceHlc { get; init; }
}

/// <summary>
/// One replication request — a batch of <see cref="ReplicationEntry"/>
/// instances for the same tree, ordered by
/// <see cref="ReplicationEntry.SourceHlc"/> ascending. Sent over HTTP
/// POST to <c>/replicate/{tree}</c>.
/// </summary>
public sealed record ReplicationBatch
{
    /// <summary>
    /// Cluster that originated the writes — stamped into the
    /// receiver's <see cref="Orleans.Runtime.RequestContext"/> before
    /// the apply calls so the receiver's outgoing call filter can
    /// suppress looping the same writes back.
    /// </summary>
    public required string SourceCluster { get; init; }

    /// <summary>Tree id the batch applies to.</summary>
    public required string Tree { get; init; }

    /// <summary>Entries to apply in order.</summary>
    public required IReadOnlyList<ReplicationEntry> Entries { get; init; }
}

/// <summary>
/// Peer acknowledgement — the receiver returns the count of entries
/// successfully applied plus the highest source HLC it observed. The
/// sender advances its <c>Cursor</c> using
/// <see cref="HighestAppliedHlc"/>.
/// </summary>
public sealed record ReplicationAck
{
    /// <summary>Number of entries the receiver applied (may be less than the batch size on partial apply).</summary>
    public required int Applied { get; init; }

    /// <summary>Highest source HLC the receiver successfully applied.</summary>
    public required HybridLogicalClock HighestAppliedHlc { get; init; }
}
