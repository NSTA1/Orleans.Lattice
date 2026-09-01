namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// A planned grain-index query, ready to enumerate. Executing it is what talks
/// to the tree: the predicate was translated, validated, and planned when
/// <see cref="IGrainIndex{TGrain, TState}.Where"/> was called, so nothing here
/// re-inspects the expression and nothing is planned per result.
/// <para>
/// A query is immutable. The <c>With</c> methods return a new query sharing the
/// same plan, so one planned query can be enumerated repeatedly, concurrently,
/// and at different page sizes.
/// </para>
/// </summary>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
public interface IGrainIndexQuery<TGrain>
    where TGrain : IGrain
{
    /// <summary>
    /// The number of entries fetched per round trip. Larger pages mean fewer
    /// round trips and more memory held per page.
    /// </summary>
    int PageSize { get; }

    /// <summary>How the query walks the tree.</summary>
    GrainIndexQueryExecution Execution { get; }

    /// <summary>
    /// Returns a query that fetches <paramref name="pageSize"/> entries per round
    /// trip.
    /// </summary>
    /// <param name="pageSize">The page size. Must be greater than zero.</param>
    /// <returns>A new query with the given page size.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    IGrainIndexQuery<TGrain> WithPageSize(int pageSize);

    /// <summary>
    /// Returns a query that walks the tree with the given execution mode.
    /// </summary>
    /// <param name="execution">The execution mode.</param>
    /// <returns>A new query with the given execution mode.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="execution"/> is not a declared mode.</exception>
    IGrainIndexQuery<TGrain> WithExecution(GrainIndexQueryExecution execution);

    /// <summary>
    /// Streams the matching grains as <see cref="IGrainFactory"/>-resolved
    /// references, each grain yielded once however many entries matched it.
    /// </summary>
    /// <param name="cancellationToken">Stops the scan.</param>
    /// <returns>The matching grain references.</returns>
    IAsyncEnumerable<TGrain> ToGrainsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams the matching grains' encoded keys, each once. This is the
    /// cheapest shape: the scan never transfers an entry payload.
    /// </summary>
    /// <param name="cancellationToken">Stops the scan.</param>
    /// <returns>The matching encoded grain keys.</returns>
    IAsyncEnumerable<string> ToKeysAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams the matching grains with the index entry that matched each one.
    /// </summary>
    /// <param name="cancellationToken">Stops the scan.</param>
    /// <returns>The matches, one per grain.</returns>
    IAsyncEnumerable<GrainIndexMatch> ToMatchesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Drains <see cref="ToGrainsAsync"/> into a list. Convenience for a result
    /// set known to be small; prefer streaming otherwise.
    /// </summary>
    /// <param name="cancellationToken">Stops the scan.</param>
    /// <returns>The matching grain references.</returns>
    Task<IReadOnlyList<TGrain>> ToGrainListAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Drains <see cref="ToKeysAsync"/> into a list. Convenience for a result set
    /// known to be small; prefer streaming otherwise.
    /// </summary>
    /// <param name="cancellationToken">Stops the scan.</param>
    /// <returns>The matching encoded grain keys.</returns>
    Task<IReadOnlyList<string>> ToKeyListAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports whether the query matches at least one grain, stopping at the
    /// first match rather than draining the scan.
    /// </summary>
    /// <param name="cancellationToken">Stops the scan.</param>
    /// <returns><c>true</c> when at least one grain matches.</returns>
    Task<bool> AnyAsync(CancellationToken cancellationToken = default);
}
