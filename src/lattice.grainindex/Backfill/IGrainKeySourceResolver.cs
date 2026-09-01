namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// Resolves the <see cref="IGrainKeySource"/> an index's background backfill
/// crawls, by index name.
/// </summary>
/// <remarks>
/// The registration is a keyed singleton, so the lookup needs the index name and
/// the index name is only known at activation. Rather than have the backfill
/// grain reach into the service provider itself, that lookup sits behind this
/// one-method seam: the grain then has a substitutable dependency and the
/// keyed-service detail stays with the registration code that chose it.
/// </remarks>
internal interface IGrainKeySourceResolver
{
    /// <summary>
    /// The key source registered for <paramref name="indexName"/>, or
    /// <c>null</c> when the host registered none.
    /// </summary>
    /// <param name="indexName">The index name. Must not be <c>null</c>.</param>
    /// <returns>The key source, or <c>null</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    IGrainKeySource? Resolve(string indexName);
}
