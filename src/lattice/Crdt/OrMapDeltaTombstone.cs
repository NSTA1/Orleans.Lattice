namespace Orleans.Lattice;

/// <summary>
/// A single per-key observed-removed dot inside an
/// <see cref="OrMapDelta{TKey, TValue}"/>. Carries the map key plus the
/// <c>(replicaId, counter)</c> dot whose corresponding add the
/// originator has now observed-as-removed; the receiver folds it into
/// the local tombstone list to cancel the matching entry on the next
/// <see cref="Orleans.Lattice.Primitives.OrMap{TKey, TValue}.Get(TKey)"/>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrMapDeltaTombstone)]
[Immutable]
public readonly record struct OrMapDeltaTombstone<TKey>
    where TKey : notnull
{
    /// <summary>The map key the tombstone is attached to.</summary>
    [Id(0)] public TKey Key { get; init; }

    /// <summary>The id of the replica that authored the cancelled dot.</summary>
    [Id(1)] public string ReplicaId { get; init; }

    /// <summary>The counter of the cancelled dot.</summary>
    [Id(2)] public long Counter { get; init; }
}