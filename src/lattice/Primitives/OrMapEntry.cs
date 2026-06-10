namespace Orleans.Lattice;

/// <summary>
/// A single dot-tagged value slot inside an
/// <see cref="OrMap{TKey, TValue}"/>: a
/// <c>(<see cref="ReplicaId"/>, <see cref="Counter"/>, <see cref="Value"/>)</c>
/// triple stamped at the moment a write was authored. The dot context
/// (<see cref="ReplicaId"/> + <see cref="Counter"/>) is what makes the
/// observed-remove map converge under concurrent active-active updates:
/// a remove tombstones every dot it observed for a key, so a concurrent
/// add on another replica with a distinct dot survives the merge with
/// its <see cref="Value"/> recursively merged through
/// <see cref="ICrdt{TSelf}"/>.
/// </summary>
/// <typeparam name="TValue">
/// The recursively-mergeable value CRDT. Must implement
/// <see cref="ICrdt{TSelf}"/> so the surrounding
/// <see cref="OrMap{TKey, TValue}"/> can fold concurrent dot values
/// together rather than discarding all but one.
/// </typeparam>
[GenerateSerializer]
[Alias(TypeAliases.OrMapEntry)]
public sealed class OrMapEntry<TValue> where TValue : ICrdt<TValue>, new()
{
    /// <summary>The id of the replica that authored this dot.</summary>
    [Id(0)]
    public string ReplicaId { get; set; } = string.Empty;

    /// <summary>The replica-local monotonic counter at the moment the dot was authored.</summary>
    [Id(1)]
    public long Counter { get; set; }

    /// <summary>The CRDT value stamped under this dot.</summary>
    [Id(2)]
    public TValue Value { get; set; } = new();
}
