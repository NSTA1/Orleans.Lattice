using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

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
    /// Observed-remove set. Receivers merge the full <see cref="Orleans.Lattice.OrSet"/>
    /// state carried by the value bytes (the producer authored the value
    /// through <see cref="CrdtLatticeExtensions.OrSet(ILattice, string)"/>,
    /// which serialises the post-write set state). State-based merge is
    /// commutative, associative, and idempotent - concurrent active-active
    /// adds and removes from multiple clusters survive convergence with
    /// their causal dot context preserved.
    /// </summary>
    OrSet = 1,

    /// <summary>
    /// Positive-negative counter. Receivers merge the full
    /// <see cref="Orleans.Lattice.PnCounter"/> state carried by the value bytes
    /// (the producer authored the value through
    /// <see cref="CrdtLatticeExtensions.PnCounter(ILattice, string)"/>) by
    /// pointwise-max on each replica's positive and negative components.
    /// Concurrent active-active increments and decrements from multiple
    /// clusters sum correctly without per-replica rendezvous.
    /// </summary>
    PnCounter = 2,

    /// <summary>
    /// Version vector. Receivers merge the full
    /// <see cref="Orleans.Lattice.VersionVector"/> state carried by the value
    /// bytes (the producer authored the value through
    /// <see cref="CrdtLatticeExtensions.VersionVector(ILattice, string)"/>)
    /// by pointwise-max on each replica's <see cref="Orleans.Lattice.HybridLogicalClock"/>
    /// entry. Late or duplicate delivery is a no-op.
    /// </summary>
    VersionVector = 3,

    /// <summary>
    /// Multi-value register. Receivers merge the full
    /// <see cref="Orleans.Lattice.MvRegister"/> state carried by the value bytes
    /// (the producer authored the value through
    /// <see cref="CrdtLatticeExtensions.MvRegister{T}(ILattice, string, ILatticeSerializer{T}?)"/>)
    /// by keeping entries whose dots are not dominated by the other side's
    /// dot context and taking the pointwise-max of the two contexts.
    /// Concurrent active-active writes from different clusters survive the
    /// merge as distinct dot-tagged values so application code can resolve
    /// the conflict rather than the wire contract silently dropping one
    /// side as <see cref="LwwRegister"/> would.
    /// </summary>
    MvRegister = 4,

    /// <summary>
    /// Observed-remove map keyed by <c>TKey</c> with recursively-mergeable
    /// <c>TValue</c> CRDT values. Receivers fold the typed
    /// <see cref="OrMapDelta{TKey, TValue}"/> carried in
    /// <see cref="WalRecord.Delta"/> into the loaded
    /// <see cref="Orleans.Lattice.OrMap{TKey, TValue}"/> via its
    /// instance <c>MergeDelta</c> method, recursing into the value CRDT's
    /// own <see cref="Orleans.Lattice.ICrdt{TSelf}.MergeFrom(TSelf)"/> for
    /// concurrent same-key writes. Because the wire shape is generic, the
    /// host must register the <c>(TKey, TValue)</c> pair through
    /// <c>AddOrMapShape&lt;TKey, TValue&gt;()</c> on the
    /// service collection before the silo starts; the producer-side
    /// accessor and receiver-side applier both look the pair up by
    /// tree id and route the deserialise + merge through the matching
    /// descriptor. Trees configured for
    /// <see cref="OrMap"/> with no registered shape descriptor fault
    /// the apply path so the misconfiguration is surfaced rather than
    /// silently dropping the entry.
    /// </summary>
    OrMap = 5,

    /// <summary>
    /// Replicated Growable Array (RGA) sequence. Receivers fold the typed
    /// <see cref="RgaDelta"/> carried in <see cref="WalRecord.Delta"/> into
    /// the loaded <see cref="Orleans.Lattice.Rga"/> via its instance
    /// <c>MergeDelta</c> method (the producer authored the delta through
    /// <see cref="CrdtLatticeExtensions.Sequence{T}(ILattice, string, ILatticeSerializer{T}?)"/>).
    /// Each insert ships the dot-explicit triple <c>(dot, parentDot,
    /// value)</c> and each remove ships the tombstoned dot, so concurrent
    /// active-active inserts and deletes from multiple clusters converge
    /// on an identical ordered traversal via the standard descending
    /// <c>(Counter, ReplicaId)</c> sibling tie-break - replaying the
    /// post-merge materialised order instead would lose the
    /// concurrent-insert information the sequence needs to converge. The
    /// descriptor is a global closed shape, so no per-tree registration is
    /// required.
    /// </summary>
    Sequence = 6,

    /// <summary>
    /// Observed-remove (enable-wins) flag. Each key carries an
    /// <see cref="Orleans.Lattice.OrFlag"/> whose state is the set of
    /// enable dots minus the set of observed-remove (disable) dots; the
    /// flag is present when at least one enable dot survives. Receivers
    /// fold the typed <see cref="OrFlagDelta"/> carried in
    /// <see cref="WalRecord.Delta"/> into the loaded
    /// <see cref="Orleans.Lattice.OrFlag"/> via its instance
    /// <c>MergeDelta</c> method (the producer authored the delta through
    /// <see cref="CrdtLatticeExtensions.OrFlag(ILattice, string)"/>).
    /// Concurrent active-active enable and disable of the same key from
    /// different clusters converge add-wins with their causal dot context
    /// preserved - a disable cancels only the enable dots it observed, so
    /// a concurrent enable on another replica survives. This is the
    /// minimal observed-remove primitive for composite-key membership
    /// rows (e.g. a tag/key secondary index) where the meaningful bit is
    /// the row's presence rather than its value, giving OR-Set-grade
    /// semantics without carrying a singleton set's element payload. The
    /// descriptor is a global closed shape, so no per-tree registration
    /// is required.
    /// </summary>
    OrFlag = 7,

    /// <summary>
    /// Remove-wins (disable-wins) flag - the inverse of
    /// <see cref="OrFlag"/>. Each key carries a
    /// <see cref="Orleans.Lattice.RwFlag"/> whose state is a set of enable
    /// dots, a set of disable (remove) dots, and a set of observed-enable
    /// tombstones cancelling disables; the flag is present only when at
    /// least one enable dot exists and no disable dot survives. Receivers
    /// fold the typed <see cref="RwFlagDelta"/> carried in
    /// <see cref="WalRecord.Delta"/> into the loaded
    /// <see cref="Orleans.Lattice.RwFlag"/> via its instance
    /// <c>MergeDelta</c> method (the producer authored the delta through
    /// <see cref="CrdtLatticeExtensions.RwFlag(ILattice, string)"/>).
    /// Concurrent active-active enable and disable of the same key from
    /// different clusters converge remove-wins with their causal dot context
    /// preserved - a disable an enable has not observed survives and keeps
    /// the flag off, so a revoke is never silently resurrected by a
    /// concurrent re-add. This is the remove-wins counterpart of
    /// <see cref="OrFlag"/> for composite-key membership rows (e.g. a
    /// tag/key secondary index, revocation list, or blocklist) where a
    /// removal must win the tie. The descriptor is a global closed shape,
    /// so no per-tree registration is required.
    /// </summary>
    RwFlag = 8,
}
