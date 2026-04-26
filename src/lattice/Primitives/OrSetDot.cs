namespace Orleans.Lattice.Primitives;

/// <summary>
/// A single causally-tagged add in an <see cref="OrSet"/>: a unique
/// <c>(<see cref="ReplicaId"/>, <see cref="Counter"/>)</c> dot stamped at
/// the moment the add was authored. The dot context is what gives an OR-Set
/// its convergence under concurrent active-active updates — a remove deletes
/// only the dots it observed, so a concurrent add on another replica with a
/// distinct dot survives the merge.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrSetDot)]
[Immutable]
public readonly record struct OrSetDot
{
    /// <summary>The id of the replica that authored this dot.</summary>
    [Id(0)] public string ReplicaId { get; init; }

    /// <summary>The replica-local monotonic counter at the moment the dot was authored.</summary>
    [Id(1)] public long Counter { get; init; }
}
