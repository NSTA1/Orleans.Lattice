using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Single item in a batched typed-CRDT delta-apply request submitted via
/// <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyCrdtDeltaManyAsync"/>. Carries
/// the authoring cluster's metadata (<see cref="SourceHlc"/>,
/// <see cref="OriginClusterId"/>, <see cref="SourceVectorClock"/>) plus
/// the typed CRDT delta bytes and the convergence
/// <see cref="LatticeMergeMode"/> verbatim, so the receiver folds the
/// per-replica delta into its current visible state under the source's
/// HLC and origin - the batch-path equivalent of one per-entry
/// <c>ApplyCrdtDeltaAsync</c> apply.
/// <para>
/// CRDT folds are commutative, associative, and idempotent, so the items
/// in one batch may be applied in any order and a re-delivered item folds
/// to the same converged state; this is what lets the receiver collapse a
/// run of typed-delta applies into a single grain RPC without the
/// per-entry optimistic-concurrency retry loop the historical
/// read-merge-write path required.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ApplyCrdtDeltaItem)]
[Immutable]
internal readonly record struct ApplyCrdtDeltaItem
{
    /// <summary>The key the remote CRDT delta targeted.</summary>
    [Id(0)] public string Key { get; init; }

    /// <summary>
    /// The convergence rule the receiver folds the delta under. Always a
    /// CRDT mode (never <see cref="LatticeMergeMode.LwwRegister"/>); LWW
    /// writes ride <see cref="Orleans.Lattice.BPlusTree.IReplicationApplyGrain.ApplyMergeManyAsync"/>
    /// instead.
    /// </summary>
    [Id(1)] public LatticeMergeMode Mode { get; init; }

    /// <summary>
    /// The typed CRDT delta bytes authored by the remote cluster, in the
    /// registered shape's delta wire format. Folded into the receiver's
    /// current visible state via the registered <c>CrdtShape</c>.
    /// </summary>
    [Id(2)] public byte[] Delta { get; init; }

    /// <summary>The HLC stamped by the remote cluster.</summary>
    [Id(3)] public HybridLogicalClock SourceHlc { get; init; }

    /// <summary>The id of the remote cluster that authored the delta.</summary>
    [Id(4)] public string OriginClusterId { get; init; }

    /// <summary>
    /// The vector-clock frontier captured by the remote cluster at commit
    /// time, or <see langword="null"/> when the producing cluster does not
    /// stamp a frontier. Stamped verbatim onto the persisted
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.VectorClock"/>.
    /// </summary>
    [Id(5)] public VersionVector? SourceVectorClock { get; init; }
}
