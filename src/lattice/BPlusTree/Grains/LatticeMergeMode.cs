using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Declared convergence rule for a tree. Authored once at configuration
/// time on the per-tree replication map (when the replication package is
/// registered) and stamped onto every captured <see cref="WalRecord"/> by
/// the commit-time path so receivers know which apply algorithm to use.
/// <para>
/// There is no implicit fallback: a tree that is not declared in the
/// replication map is not replicated. This is by design - the core library
/// stores every value as opaque <c>byte[]</c>, so the producer cannot
/// recognise CRDT primitives by inspection. Mode declaration is the only
/// way the observer can know how a receiver is meant to merge the value.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeMergeMode)]
public enum LatticeMergeMode
{
    /// <summary>
    /// Last-writer-wins on the value bytes, ordered by
    /// <c>(HybridLogicalClock, OriginClusterId)</c>. Concurrent writes from
    /// different clusters silently drop the loser - safe only when the
    /// application maintains single-writer-per-key discipline (each key has
    /// one authoritative cluster at any given time, e.g. a routed-by-tenant
    /// or routed-by-shard topology). Use a typed CRDT mode below if
    /// concurrent writes from multiple clusters can target the same key.
    /// </summary>
    LwwRegister = 0,

    /// <summary>
    /// Observed-remove set. Receivers merge the full <see cref="Primitives.OrSet"/>
    /// state carried by the value bytes (the producer authored the value
    /// through <see cref="CrdtLatticeExtensions.OrSet(ILattice, string)"/>,
    /// which serialises the post-write set state). State-based merge is
    /// commutative, associative, and idempotent — concurrent active-active
    /// adds and removes from multiple clusters survive convergence with
    /// their causal dot context preserved.
    /// </summary>
    OrSet = 1,

    /// <summary>
    /// Positive-negative counter. Receivers merge the full
    /// <see cref="Primitives.PnCounter"/> state carried by the value bytes
    /// (the producer authored the value through
    /// <see cref="CrdtLatticeExtensions.PnCounter(ILattice, string)"/>) by
    /// pointwise-max on each replica's positive and negative components.
    /// Concurrent active-active increments and decrements from multiple
    /// clusters sum correctly without per-replica rendezvous.
    /// </summary>
    PnCounter = 2,

    /// <summary>
    /// Version vector. Receivers merge the full
    /// <see cref="Primitives.VersionVector"/> state carried by the value
    /// bytes (the producer authored the value through
    /// <see cref="CrdtLatticeExtensions.VersionVector(ILattice, string)"/>)
    /// by pointwise-max on each replica's <see cref="Primitives.HybridLogicalClock"/>
    /// entry. Late or duplicate delivery is a no-op.
    /// </summary>
    VersionVector = 3,
}
