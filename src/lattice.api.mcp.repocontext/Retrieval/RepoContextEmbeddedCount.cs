namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The embedded-source count for one repository as
/// <see cref="RepoContextVectorWriter.CountEmbeddedAsync"/> can serve it without
/// blocking: a memoised value plus an honest statement of how current it is.
/// </summary>
/// <remarks>
/// <para>
/// <c>embeddedVectorCount</c> is a progress statistic rendered by
/// <c>repocontext_list_repos</c>, never an input to a correctness decision, and the
/// only way to compute it exactly is to walk the whole membership tree - the largest
/// and slowest tree in the store. Because any membership write advances the cache
/// generation the memo is keyed by, an active back-fill invalidates it continuously,
/// so an exact-on-every-call contract turned a routine metadata call into an O(tree)
/// scan per repository (issue 1992).
/// </para>
/// <para>
/// The trade is therefore inverted deliberately: the count is served from the last
/// completed scan and refreshed out of band, and this type carries the staleness
/// rather than hiding it. During a back-fill an "exact" count is stale the instant it
/// is computed anyway, so the strictness bought nothing and cost the whole tool.
/// </para>
/// </remarks>
public readonly record struct RepoContextEmbeddedCount
{
    /// <summary>
    /// The last completed count of live embedded sources, or <see langword="null"/>
    /// when no scan has completed for the repository yet, so the count is not yet
    /// known. A <see langword="null"/> count is never reported as <c>0</c>: "nothing
    /// embedded" and "not measured yet" are different answers and an operator
    /// watching a back-fill needs to tell them apart.
    /// </summary>
    public long? Count { get; init; }

    /// <summary>
    /// Whether a refresh is outstanding, so <see cref="Count"/> is a snapshot from an
    /// earlier membership generation (or absent entirely) rather than the current one.
    /// <see langword="false"/> means the value was computed against the membership
    /// generation the caller observed and is exact as of that generation.
    /// </summary>
    public bool Pending { get; init; }

    /// <summary>An exact count computed against the caller's membership generation.</summary>
    /// <param name="count">The counted live embedded sources.</param>
    /// <returns>An exact, non-pending count.</returns>
    public static RepoContextEmbeddedCount Exact(long count)
        => new() { Count = count, Pending = false };

    /// <summary>
    /// A count carried over from an earlier generation while a refresh is outstanding.
    /// </summary>
    /// <param name="count">The last completed count, or <see langword="null"/> when none has completed.</param>
    /// <returns>A pending count.</returns>
    public static RepoContextEmbeddedCount PendingRefresh(long? count)
        => new() { Count = count, Pending = true };
}
