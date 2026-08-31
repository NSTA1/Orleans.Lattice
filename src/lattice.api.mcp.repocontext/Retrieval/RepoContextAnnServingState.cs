namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Which path inside the approximate retrieval plane actually answered a query.
/// This is the operator-facing build-state report that sits <b>beside</b> the
/// caller-facing <see cref="RepoContextRetrievalPath"/> vocabulary, not inside
/// it: the response vocabulary is a closed set of five values describing the
/// recall guarantee a caller received, while this describes how the plane got
/// there.
/// <para>
/// Two of the three states answer with <b>complete</b> recall and are reported
/// to the caller as <see cref="RepoContextRetrievalPath.SemanticApproximate"/>
/// anyway, because the declaration is per index rather than per query and
/// under-promising recall is the only safe direction. None of the three is a
/// degradation: a warming plane is slower, never wrong, and must never be
/// reported as <see cref="RepoContextRetrievalPath.KeywordIndexDegraded"/>.
/// </para>
/// </summary>
internal enum RepoContextAnnServingState
{
    /// <summary>
    /// No usable approximate index exists yet for this repository and embedding
    /// space, so the exact scan answered. Recall is complete and the query cost
    /// is the pre-change cost. This is the expected state of an existing
    /// deployment between its first start on a new build and the completion of
    /// the background build.
    /// </summary>
    Bootstrapping = 0,

    /// <summary>
    /// The approximate index answered, but it holds no usable partitioning yet
    /// (its corpus is below the training threshold, or training has not run), so
    /// it answered by exhaustive scan of the vectors it holds. Recall over the
    /// indexed corpus is complete; the index is warming up, not degraded.
    /// </summary>
    Exhaustive = 1,

    /// <summary>
    /// The approximate index answered from its trained partitioning. Recall is
    /// bounded by the published target and query cost is sub-linear in the
    /// corpus. This is the steady state.
    /// </summary>
    Approximate = 2,
}
