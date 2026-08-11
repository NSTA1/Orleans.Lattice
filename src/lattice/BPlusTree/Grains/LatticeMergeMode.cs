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

    /// <summary>
    /// Grow-only counter. Receivers fold the typed <see cref="GCounterDelta"/>
    /// carried in <see cref="WalRecord.Delta"/> into the loaded
    /// <see cref="Orleans.Lattice.GCounter"/> via its instance
    /// <c>MergeDelta</c> method (the producer authored the delta through
    /// <see cref="CrdtLatticeExtensions.GCounter(ILattice, string)"/>) by
    /// pointwise-max on each replica's cumulative component. This is the
    /// monotonic-only counter that <see cref="PnCounter"/> is built from - the
    /// natural primitive for monotone metrics, sequence / event counters, and
    /// quota consumption where decrement never happens. Concurrent active-active
    /// increments from multiple clusters sum correctly without per-replica
    /// rendezvous, and late or duplicate delivery is an idempotent no-op. The
    /// descriptor is a global closed shape, so no per-tree registration is
    /// required.
    /// </summary>
    GCounter = 9,

    /// <summary>
    /// Grow-only (G) set. Each key carries a <see cref="Orleans.Lattice.GSet"/>
    /// whose state is a set of opaque element byte arrays with value-equality
    /// by content. Receivers fold the typed <see cref="GSetDelta"/> carried in
    /// <see cref="WalRecord.Delta"/> into the loaded
    /// <see cref="Orleans.Lattice.GSet"/> via its instance <c>MergeDelta</c>
    /// method (the producer authored the delta through
    /// <see cref="CrdtLatticeExtensions.GSet(ILattice, string)"/>). The merge
    /// is set union, which is trivially commutative, associative, and
    /// idempotent, so concurrent active-active adds from multiple clusters all
    /// survive convergence. The set is grow-only by design - it carries no
    /// dots and no tombstones and has no remove operation, so it is the minimal
    /// set primitive for append-only workloads (tag sets, seen-ids,
    /// accumulating audiences); reach for <see cref="OrSet"/> when removal is
    /// needed. The descriptor is a global closed shape, so no per-tree
    /// registration is required.
    /// </summary>
    GSet = 10,

    /// <summary>
    /// Remove-wins observed-remove set - the set-granularity generalisation
    /// of <see cref="RwFlag"/> (an <see cref="RwFlag"/> is a single-element
    /// <see cref="RwSet"/>, exactly as <see cref="OrFlag"/> is to
    /// <see cref="OrSet"/>). Each key carries a
    /// <see cref="Orleans.Lattice.RwSet"/> whose state keeps, per element, a
    /// set of add dots, a set of remove dots, and a set of observed-add
    /// tombstones cancelling removes; an element is a member only when it
    /// carries an add dot and no remove dot survives. Receivers fold the
    /// typed <see cref="RwSetDelta"/> carried in
    /// <see cref="WalRecord.Delta"/> into the loaded
    /// <see cref="Orleans.Lattice.RwSet"/> via its instance
    /// <c>MergeDelta</c> method (the producer authored the delta through
    /// <see cref="CrdtLatticeExtensions.RwSet(ILattice, string)"/>).
    /// Concurrent active-active add and remove of the same element from
    /// different clusters converge remove-wins with their causal dot context
    /// preserved - a remove an add has not observed survives and keeps the
    /// element out, so a revoke is never silently resurrected by a concurrent
    /// re-add. This is the remove-wins counterpart of the add-wins
    /// <see cref="OrSet"/>, the natural primitive for membership revocation
    /// lists and blocklists where a removal must win the tie. The descriptor
    /// is a global closed shape, so no per-tree registration is required.
    /// </summary>
    RwSet = 11,
    /// Monotone max register - keeps the greatest totally-ordered value ever
    /// seen. Each key carries a <see cref="Orleans.Lattice.BoundedRegister"/>
    /// whose state is a single value paired with an explicit total-order key;
    /// receivers fold the typed <see cref="BoundedRegisterDelta"/> carried in
    /// <see cref="WalRecord.Delta"/> into the loaded register via its instance
    /// <c>MergeDelta</c> method (the producer authored the delta through
    /// <see cref="CrdtLatticeExtensions.MaxRegister{T}(ILattice, string, System.Func{T, byte[]}, ILatticeSerializer{T}?)"/>).
    /// The fold is directional max over the total order carried on the wire, so
    /// it is commutative, associative, and idempotent - a backwards write or a
    /// duplicate delivery is a no-op, and concurrent active-active writes from
    /// different clusters converge on the single greatest value without needing
    /// the domain comparer on the receiver. This is the high-water-mark
    /// primitive (a monotone gauge, a version ceiling, a max-seen reading). The
    /// descriptor is a global closed shape, so no per-tree registration is
    /// required.
    /// </summary>
    MaxRegister = 12,

    /// <summary>
    /// Monotone min register - the inverse of <see cref="MaxRegister"/>, keeping
    /// the smallest totally-ordered value ever seen. Each key carries a
    /// <see cref="Orleans.Lattice.BoundedRegister"/> whose state is a single
    /// value paired with an explicit total-order key; receivers fold the typed
    /// <see cref="BoundedRegisterDelta"/> carried in
    /// <see cref="WalRecord.Delta"/> into the loaded register via its instance
    /// <c>MergeDelta</c> method (the producer authored the delta through
    /// <see cref="CrdtLatticeExtensions.MinRegister{T}(ILattice, string, System.Func{T, byte[]}, ILatticeSerializer{T}?)"/>).
    /// The fold is directional min over the total order carried on the wire, so
    /// it is commutative, associative, and idempotent - a backwards write or a
    /// duplicate delivery is a no-op, and concurrent active-active writes from
    /// different clusters converge on the single smallest value without needing
    /// the domain comparer on the receiver. This is the low-water-mark primitive
    /// (a min-seen latency floor, a first-seen timestamp). The descriptor is a
    /// global closed shape, so no per-tree registration is required.
    /// </summary>
    MinRegister = 13,
}
