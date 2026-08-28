namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for an observed-remove (OR) map mutation. Carries
/// the dot-tagged value snapshots added and the dots removed since the
/// receiver's cursor, generic over the map key and value-CRDT types so
/// the public wire contract is type-safe across the producer/receiver
/// boundary.
/// <para>
/// Apply semantics on the receiver mirror <see cref="Orleans.Lattice.OrMap{TKey, TValue}.MergeFrom(Orleans.Lattice.OrMap{TKey, TValue})"/>:
/// union <see cref="Adds"/> into the local per-key entry lists (per-dot
/// dedup, same-dot collisions lattice-merged through the value CRDT),
/// then union <see cref="Tombstones"/> into the per-key tombstone lists.
/// The result is independent of arrival order, duplicate delivery, and
/// partial overlap with the local state.
/// </para>
/// <para>
/// Emitters always populate both collections (use empty arrays for
/// "no adds" / "no removes"); use <see cref="Empty"/> to author a no-op
/// delta without allocating fresh empty arrays. The <c>default</c>
/// instance has <c>null</c> collections and is intended only as the
/// zero-value of the struct - consumers should either treat <c>null</c>
/// as empty or assert non-null at the apply boundary.
/// </para>
/// <para>
/// Deliberately <em>not</em> <c>[Immutable]</c>: <see cref="Adds"/> carries
/// <see cref="OrMapDeltaEntry{TKey, TValue}"/> values whose payload is a mutable
/// CRDT instance the receiver folds in place, so Orleans must deep-copy the
/// delta on a same-silo grain call rather than hand the receiver the producer's
/// own objects.
/// </para>
/// </summary>
/// <typeparam name="TKey">
/// The map key type. Must support reasonable dictionary equality
/// (e.g. <see cref="string"/>, <see cref="int"/>, <see cref="System.Guid"/>).
/// </typeparam>
/// <typeparam name="TValue">
/// The recursively-mergeable value CRDT, constrained by
/// <see cref="Orleans.Lattice.ICrdt{TSelf}"/> with a public
/// parameterless constructor.
/// </typeparam>
[GenerateSerializer]
[Alias(TypeAliases.OrMapDelta)]
public readonly record struct OrMapDelta<TKey, TValue>
    where TKey : notnull
    where TValue : Orleans.Lattice.ICrdt<TValue>, new()
{
    /// <summary>
    /// The per-key dot-tagged value snapshots added since the
    /// receiver's cursor. Each entry carries the key, the
    /// <c>(replicaId, counter)</c> dot, and the value-CRDT snapshot
    /// attached to that dot.
    /// </summary>
    [Id(0)] public IReadOnlyList<OrMapDeltaEntry<TKey, TValue>> Adds { get; init; }

    /// <summary>
    /// The per-key tombstones observed since the receiver's cursor.
    /// Each entry carries the key plus the <c>(replicaId, counter)</c>
    /// dot whose corresponding add the originator has now observed-as-
    /// removed.
    /// </summary>
    [Id(1)] public IReadOnlyList<OrMapDeltaTombstone<TKey>> Tombstones { get; init; }

    /// <summary>
    /// A reusable no-op delta with empty (but non-null) <see cref="Adds"/>
    /// and <see cref="Tombstones"/> collections. Backed by
    /// <see cref="System.Array.Empty{T}"/> so repeated access does not allocate.
    /// </summary>
    public static OrMapDelta<TKey, TValue> Empty { get; } = new()
    {
        Adds = System.Array.Empty<OrMapDeltaEntry<TKey, TValue>>(),
        Tombstones = System.Array.Empty<OrMapDeltaTombstone<TKey>>(),
    };
}