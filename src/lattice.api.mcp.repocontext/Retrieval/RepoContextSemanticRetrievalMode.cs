namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Which semantic retrieval path a host wants bound: the persisted approximate
/// nearest-neighbour index (the default), or the brute-force exact scan that
/// preceded it.
/// <para>
/// The distinction is a genuine contract difference, not a tuning knob, which is
/// why it is configuration rather than an internal heuristic. Exact recall is
/// complete but its cost is proportional to the corpus, so every cold query has
/// to re-activate every leaf of the vector-metadata tree. Approximate recall is
/// bounded - published floors of <c>recall@10 &gt;= 0.95</c> on a clustered
/// corpus and <c>&gt;= 0.55</c> on an adversarially unclustered one - but its
/// cost is sub-linear and it survives a restart, which is what bounds cold start.
/// Whichever is bound, the answer reports itself honestly through
/// <see cref="RepoContextRetrievalPath"/>, so a caller is never left guessing
/// which guarantee it received.
/// </para>
/// </summary>
internal enum RepoContextSemanticRetrievalMode
{
    /// <summary>
    /// Route semantic retrieval through the persisted approximate index. The
    /// default. Answers report
    /// <see cref="RepoContextRetrievalPath.SemanticApproximate"/>.
    /// </summary>
    Approximate = 0,

    /// <summary>
    /// Route semantic retrieval through the brute-force exact scan. Answers
    /// report <see cref="RepoContextRetrievalPath.SemanticExact"/>. Recall is
    /// complete and query cost is proportional to the corpus.
    /// </summary>
    Exact = 1,
}
