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
}
