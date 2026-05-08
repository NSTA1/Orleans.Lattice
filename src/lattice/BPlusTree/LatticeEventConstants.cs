namespace Orleans.Lattice;

/// <summary>
/// Shared constants for the Lattice event-stream subsystem.
/// </summary>
public static class LatticeEventConstants
{
    /// <summary>
    /// Orleans stream namespace for every <see cref="LatticeTreeEvent"/>. Stream
    /// id within this namespace is the logical tree id, so one stream exists
    /// per tree.
    /// </summary>
    public const string StreamNamespace = "orleans.lattice.events";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to propagate a saga's
    /// <c>operationId</c> through each per-key write it makes. Internal —
    /// consumers should read <see cref="LatticeTreeEvent.OperationId"/> instead.
    /// </summary>
    internal const string OperationIdRequestContextKey = "ol.opid";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to carry the origin cluster
    /// identifier from an inbound replication / forwarding handler down
    /// into <see cref="BPlusTree.Grains.BPlusLeafGrain"/>'s write methods
    /// so the committed <c>LwwValue</c> / <c>LatticeMutation</c> can
    /// record where the mutation was authored. Public callers set this
    /// through <see cref="LatticeOriginContext"/>; they should never
    /// touch this key directly.
    /// </summary>
    internal const string OriginClusterIdRequestContextKey = "ol.ocid";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to carry the vector-clock
    /// frontier from an inbound replication / forwarding handler down
    /// into <see cref="BPlusTree.Grains.BPlusLeafGrain"/>'s write methods
    /// so the committed <c>LwwValue</c> records the frontier observed at
    /// commit time. Public callers set this through
    /// <see cref="LatticeVectorClockContext"/>; they should never touch
    /// this key directly.
    /// </summary>
    internal const string VectorClockRequestContextKey = "ol.vc";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to carry the per-transaction
    /// identifier (a <see cref="System.Guid"/>) from the public
    /// <see cref="ILattice"/> entry-point — or, in the saga case, the
    /// <see cref="BPlusTree.Grains.AtomicWriteGrain"/> coordinator — down
    /// into <see cref="BPlusTree.Grains.BPlusLeafGrain"/> /
    /// <see cref="BPlusTree.Grains.ShardRootGrain"/> mutation publish
    /// helpers. Stamped onto every emitted
    /// <see cref="LatticeMutation.TransactionId"/> so replication-aware
    /// observers can capture per-transaction state once and apply it to
    /// every emit in the batch. Internal — consumers should read
    /// <see cref="LatticeMutation.TransactionId"/> instead.
    /// </summary>
    internal const string TransactionIdRequestContextKey = "ol.txid";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to flag the current logical
    /// call as a library-internal maintenance write (resize / rebalance /
    /// compaction / internal rewrite). When the key is present and set to
    /// <c>true</c> the publish helpers stamp
    /// <see cref="LatticeMutation.Category"/> as
    /// <see cref="MutationCategory.Maintenance"/>; otherwise emits default
    /// to <see cref="MutationCategory.User"/>. Internal — set through
    /// <see cref="LatticeMaintenanceContext"/>.
    /// </summary>
    internal const string MaintenanceRequestContextKey = "ol.maint";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to carry the
    /// <em>author's delta</em> — the pre-merge mutation the caller actually
    /// authored, encoded as a <c>(Kind, Payload)</c> opaque-bytes pair —
    /// from a public <see cref="ILattice"/> entry-point or a CRDT
    /// accessor down into the leaf / shard mutation publish helpers, so
    /// the emitted <see cref="LatticeMutation"/> records the producer's
    /// intent rather than only the post-merge <c>LwwValue</c> bytes. The
    /// opaque-bytes carry deliberately keeps the public extensibility
    /// contract independent of any replication-side typed delta DTO.
    /// Internal — set through <see cref="LatticeDeltaContext"/>.
    /// </summary>
    internal const string DeltaRequestContextKey = "ol.delta";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to flag the current logical
    /// call as being driven by the commit-log adapter rather than by a
    /// foreground caller. Set by the core library's dual-durability
    /// commit path on <see cref="BPlusTree.Grains.BPlusLeafGrain"/>
    /// around the post-commit observer publish so a downstream
    /// replication-aware observer can detect and short-circuit the
    /// would-be loop where its own input is fed back into the WAL.
    /// Internal — set through <see cref="LatticeCommitLogContext"/>.
    /// </summary>
    internal const string CommitLogSourceRequestContextKey = "ol.cls";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to carry the source-cluster
    /// <see cref="Primitives.HybridLogicalClock"/> that the leaf grain
    /// must persist verbatim on the freshly-constructed
    /// <see cref="Primitives.LwwValue{T}"/>'s timestamp slot — bypassing
    /// the standard
    /// <see cref="Primitives.HybridLogicalClock.Tick(Primitives.HybridLogicalClock)"/>
    /// behaviour — so receiver-side LWW resolution sees the authoring
    /// cluster's HLC bit-identically. The leaf still advances its local
    /// clock past the override via
    /// <see cref="Primitives.HybridLogicalClock.Merge(Primitives.HybridLogicalClock, Primitives.HybridLogicalClock)"/>
    /// to preserve local monotonicity for any subsequent foreground
    /// tick. The single supported authoring path is the receiver-side
    /// cross-cluster atomic-batch apply seam
    /// (<c>IReplicationApplyGrain.ApplyManyAtomicAsync</c>). Internal —
    /// set through <see cref="LatticeHlcOverrideContext"/>.
    /// </summary>
    internal const string HlcOverrideRequestContextKey = "ol.hlc";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to carry the
    /// atomic-transaction <c>(Size, Index)</c> pair from the
    /// <see cref="BPlusTree.Grains.AtomicWriteGrain"/> coordinator down
    /// into the per-key publish helpers so the emitted
    /// <see cref="LatticeMutation"/> carries
    /// <see cref="LatticeMutation.AtomicBatchSize"/> /
    /// <see cref="LatticeMutation.AtomicBatchIndex"/> identifying the
    /// emit's position within its enclosing atomic transaction. The
    /// saga stamps this once per per-key emit (including compensation
    /// rolls); single-key non-saga writes leave the key absent and the
    /// publish helpers stamp <c>0</c> / <c>0</c> verbatim. Internal —
    /// set through <see cref="LatticeAtomicBatchContext"/>.
    /// </summary>
    internal const string AtomicBatchRequestContextKey = "ol.batch";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to flag the current logical
    /// call as a saga prepare-phase write. When set to <c>true</c>, the
    /// leaf grain's commit pipeline routes the mutation into the per-leaf
    /// in-memory pending-transaction map (keyed by
    /// <see cref="LatticeMutation.TransactionId"/>) rather than into the
    /// visible projection; reads filter pending entries out of view. A
    /// subsequent terminal <see cref="MutationKind.TxCommit"/> or
    /// <see cref="MutationKind.TxAbort"/> mutation flips or drops the
    /// pending entries on the leaf. Internal — set through
    /// <see cref="LatticePreparedContext"/>.
    /// </summary>
    internal const string PreparedRequestContextKey = "ol.prep";

    /// <summary>
    /// Orleans <c>RequestContext</c> key used to communicate the WAL
    /// offset of the mutation currently being driven through
    /// <c>ILeafProjection.Apply</c>. The replay coordinator stamps the
    /// offset before each Apply call so the leaf can record per-prepare
    /// offsets in its pending-transaction map and clamp the projection
    /// checkpoint back to <c>min(highest contiguous Apply'd offset,
    /// (min unresolved prepare offset) - 1)</c> — preventing crash
    /// recovery from advancing past an unresolved saga prepare and
    /// silently losing its writes when the terminal mark eventually
    /// arrives. Internal — set through <see cref="LatticeApplyOffsetContext"/>.
    /// </summary>
    internal const string ApplyOffsetRequestContextKey = "ol.aoff";
}
