namespace Orleans.Lattice;

/// <summary>
/// A single per-key dot-tagged value snapshot inside an
/// <see cref="OrMapDelta{TKey, TValue}"/>. Carries the map key, the
/// <c>(replicaId, counter)</c> dot that identifies the add, and the
/// value-CRDT snapshot attached to that dot.
/// <para>
/// <strong>Equality caveat.</strong> The synthesized record-struct equality
/// delegates to the default comparer for each field. For reference-typed
/// <typeparamref name="TKey"/> or <typeparamref name="TValue"/> instances
/// (e.g. <see cref="string"/> keys plus a CRDT value class) equality
/// degrades to reference equality on the value snapshot, so callers
/// comparing entries across deltas should match on the dot tuple
/// (<see cref="ReplicaId"/>, <see cref="Counter"/>) plus the key, not
/// via record equality.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrMapDeltaEntry)]
[Immutable]
public readonly record struct OrMapDeltaEntry<TKey, TValue>
    where TKey : notnull
    where TValue : Orleans.Lattice.ICrdt<TValue>, new()
{
    /// <summary>The map key the entry is attached to.</summary>
    [Id(0)] public TKey Key { get; init; }

    /// <summary>The id of the replica that authored the dot.</summary>
    [Id(1)] public string ReplicaId { get; init; }

    /// <summary>The replica-local monotonic counter at the moment the dot was authored.</summary>
    [Id(2)] public long Counter { get; init; }

    /// <summary>The value-CRDT snapshot attached to this dot.</summary>
    [Id(3)] public TValue Value { get; init; }
}