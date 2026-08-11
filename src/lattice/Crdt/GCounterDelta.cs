namespace Orleans.Lattice;

/// <summary>
/// Typed delta record for a grow-only (G) counter mutation. Carries the
/// per-replica cumulative components that have advanced since the receiver's
/// cursor. Because each cluster only ever advances its own row and the receiver
/// merges by pointwise-max, the counter converges under concurrent
/// active-active increments without per-replica rendezvous.
/// <para>
/// Apply semantics on the receiver: for every <c>(replicaId, value)</c> pair in
/// <see cref="Increments"/>, set the local component for that replica to
/// <c>max(local, value)</c>. Never add - the value is the cumulative count, not
/// an increment to sum.
/// </para>
/// <para>
/// Emitters always populate the map (use an empty
/// <see cref="Dictionary{TKey, TValue}"/> for "no advances"); use
/// <see cref="Empty"/> to author a no-op delta without allocating a fresh
/// dictionary. The <see langword="default"/> instance has a <c>null</c> map and
/// is intended only as the zero-value of the struct.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.GCounterDelta)]
[Immutable]
public readonly record struct GCounterDelta
{
    /// <summary>
    /// Per-replica cumulative grow-only component. Each entry's value is the
    /// highest cumulative increment count observed from that replica up to the
    /// point this delta was authored.
    /// </summary>
    [Id(0)] public Dictionary<string, long> Increments { get; init; }

    /// <summary>
    /// A reusable no-op delta with an empty (but non-null)
    /// <see cref="Increments"/> map. The backing dictionary is shared - callers
    /// must not mutate it.
    /// </summary>
    public static GCounterDelta Empty { get; } = new()
    {
        Increments = new Dictionary<string, long>(0),
    };
}
