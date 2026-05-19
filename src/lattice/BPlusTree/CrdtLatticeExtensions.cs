namespace Orleans.Lattice;

/// <summary>
/// Typed CRDT value-surface accessor extensions on <see cref="ILattice"/>.
/// Each method returns a lightweight, allocation-free accessor that reads
/// and writes a single key under optimistic concurrency, exposing the
/// primitive's natural mutation API (add / remove, increment / decrement,
/// tick / merge) instead of forcing callers to hand-roll byte arrays and
/// CAS retry loops.
/// </summary>
public static class CrdtLatticeExtensions
{
    /// <summary>
    /// Returns a typed accessor for an observed-remove (OR) set stored
    /// under <paramref name="key"/> in <paramref name="lattice"/>.
    /// </summary>
    /// <param name="lattice">The tree containing the set.</param>
    /// <param name="key">The key the set is stored under.</param>
    public static OrSetAccessor OrSet(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new OrSetAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a positive-negative (PN) counter
    /// stored under <paramref name="key"/> in <paramref name="lattice"/>.
    /// </summary>
    /// <param name="lattice">The tree containing the counter.</param>
    /// <param name="key">The key the counter is stored under.</param>
    public static PnCounterAccessor PnCounter(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new PnCounterAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a version vector stored under
    /// <paramref name="key"/> in <paramref name="lattice"/>.
    /// </summary>
    /// <param name="lattice">The tree containing the vector.</param>
    /// <param name="key">The key the vector is stored under.</param>
    public static VersionVectorAccessor VersionVector(this ILattice lattice, string key)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new VersionVectorAccessor(lattice, key);
    }

    /// <summary>
    /// Returns a typed accessor for a multi-value register stored under
    /// <paramref name="key"/> in <paramref name="lattice"/>. Concurrent
    /// writes from different replicas survive the merge as distinct
    /// dot-tagged values rather than the wire contract silently dropping
    /// one side as last-writer-wins would; the application reads the
    /// conflict set via
    /// <see cref="MvRegisterAccessor{T}.ValuesAsync(CancellationToken)"/>
    /// and resolves it itself.
    /// </summary>
    /// <typeparam name="T">The user-facing value type. Serialised to and from <see cref="byte"/>[] through the supplied <paramref name="serializer"/> or <see cref="JsonLatticeSerializer{T}"/>.</typeparam>
    /// <param name="lattice">The tree containing the register.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="serializer">Optional serializer for <typeparamref name="T"/>. Defaults to <see cref="JsonLatticeSerializer{T}.Default"/>.</param>
    public static MvRegisterAccessor<T> MvRegister<T>(this ILattice lattice, string key, ILatticeSerializer<T>? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new MvRegisterAccessor<T>(lattice, key, serializer ?? JsonLatticeSerializer<T>.Default);
    }
}
