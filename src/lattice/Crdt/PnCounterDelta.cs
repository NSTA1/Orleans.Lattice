namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a positive-negative (PN) counter mutation.
/// Carries the per-replica increment and decrement components that have
/// advanced since the receiver's cursor. The split into two non-negative
/// per-replica vectors is what makes the counter convergent under
/// concurrent active-active updates: each cluster only ever advances its
/// own row in either map, and the receiver merges by pointwise-max.
/// <para>
/// Apply semantics on the receiver: for every <c>(replicaId, value)</c>
/// pair in <see cref="Increments"/>, set the local positive component
/// for that replica to <c>max(local, value)</c>; do the same for
/// <see cref="Decrements"/> against the negative component. Never
/// subtract - the value is the cumulative count, not a delta.
/// </para>
/// <para>
/// Emitters always populate both maps (use an empty <see cref="Dictionary{TKey, TValue}"/>
/// for "no advances on this side"); use <see cref="Empty"/> to author a
/// no-op delta without allocating fresh dictionaries. The
/// <see langword="default"/> instance has <c>null</c> maps and is intended
/// only as the zero-value of the struct.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.PnCounterDelta)]
[Immutable]
public readonly record struct PnCounterDelta
{
    /// <summary>
    /// Per-replica cumulative positive component. Each entry's value is
    /// the highest cumulative increment count observed from that replica
    /// up to the point this delta was authored.
    /// </summary>
    [Id(0)] public Dictionary<string, long> Increments { get; init; }

    /// <summary>
    /// Per-replica cumulative negative component. Each entry's value is
    /// the highest cumulative decrement count observed from that replica
    /// up to the point this delta was authored.
    /// </summary>
    [Id(1)] public Dictionary<string, long> Decrements { get; init; }

    /// <summary>
    /// A reusable no-op delta with empty (but non-null) <see cref="Increments"/>
    /// and <see cref="Decrements"/> maps. The backing dictionaries are
    /// shared - callers must not mutate them.
    /// </summary>
    public static PnCounterDelta Empty { get; } = new()
    {
        Increments = new Dictionary<string, long>(0),
        Decrements = new Dictionary<string, long>(0),
    };
}
