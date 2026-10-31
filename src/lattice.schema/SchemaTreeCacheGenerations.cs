namespace Orleans.Lattice.Schema;

/// <summary>
/// Per-tree invalidation generations for a schema configuration cache. A load
/// captures its tree's generation before reading the store and installs its result
/// only if no invalidation of that tree has happened since, so a load that an
/// invalidation overtook can never re-cache the pre-change answer (#5247).
/// </summary>
/// <remarks>
/// Trees are hashed onto a fixed set of stripes rather than given one counter each,
/// so the generations cost a constant amount of memory however many distinct tree
/// ids are invalidated (the cache itself is bounded for the same reason). Two trees
/// sharing a stripe only ever make a load discard a still-current answer, which
/// costs one extra store read on the next lookup and never serves a stale one.
/// </remarks>
internal sealed class SchemaTreeCacheGenerations
{
    private const int StripeCount = 64;

    private readonly long[] _stripes = new long[StripeCount];

    /// <summary>Reads the tree's current generation; call before the store read.</summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <returns>The generation to pass to <see cref="IsCurrent"/>.</returns>
    public long Capture(string treeId) => Volatile.Read(ref _stripes[Stripe(treeId)]);

    /// <summary>Reports whether no invalidation of the tree has happened since <paramref name="captured"/>.</summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="captured">The generation returned by <see cref="Capture"/>.</param>
    /// <returns><c>true</c> when a load that captured <paramref name="captured"/> may still install its result.</returns>
    public bool IsCurrent(string treeId, long captured) => Volatile.Read(ref _stripes[Stripe(treeId)]) == captured;

    /// <summary>Advances the tree's generation; call before removing its cache entry.</summary>
    /// <param name="treeId">The governed tree id.</param>
    public void Advance(string treeId) => Interlocked.Increment(ref _stripes[Stripe(treeId)]);

    /// <summary>Advances every tree's generation; call before clearing the whole cache.</summary>
    public void AdvanceAll()
    {
        for (var i = 0; i < _stripes.Length; i++)
        {
            Interlocked.Increment(ref _stripes[i]);
        }
    }

    private static int Stripe(string treeId) => (int)((uint)treeId.GetHashCode() % StripeCount);
}
