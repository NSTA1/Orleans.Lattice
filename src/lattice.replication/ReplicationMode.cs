namespace Orleans.Lattice.Replication;

/// <summary>
/// Declared convergence rule for a replicated tree. Authored once at
/// configuration time on <see cref="LatticeReplicationOptions.ReplicatedTrees"/>;
/// stamped onto every captured <see cref="ReplogEntry"/> by the commit-time
/// observer so receivers know which apply algorithm to use.
/// <para>
/// There is no implicit fallback: a tree that is not declared in
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> is not replicated.
/// This is by design - the core library stores every value as opaque
/// <c>byte[]</c>, so the producer cannot recognise CRDT primitives by
/// inspection. Mode declaration is the only way the observer can know how a
/// receiver is meant to merge the value.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ReplicationMode)]
public enum ReplicationMode
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
    /// Observed-remove set. Reserved for a future release; declaring a tree
    /// with this mode is rejected by the options validator until the core
    /// library exposes the typed primitive value surface that lets the
    /// observer recognise an OR-Set value at commit time.
    /// </summary>
    OrSet = 1,

    /// <summary>
    /// Positive-negative counter. Reserved for a future release; declaring
    /// a tree with this mode is rejected by the options validator until the
    /// core library exposes the typed primitive value surface that lets the
    /// observer recognise a PN-Counter value at commit time.
    /// </summary>
    PnCounter = 2,

    /// <summary>
    /// Version vector. Reserved for a future release; declaring a tree
    /// with this mode is rejected by the options validator until the core
    /// library exposes the typed primitive value surface that lets the
    /// observer recognise a version-vector value at commit time.
    /// </summary>
    VersionVector = 3,
}
