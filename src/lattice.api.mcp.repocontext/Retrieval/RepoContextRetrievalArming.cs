namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Whether the approximate retrieval plane is <b>armed</b> - answering from a
/// trained partitioning - as last demonstrated by a query that the plane itself
/// answered.
/// <para>
/// <b>Why this is separate from readiness.</b>
/// <see cref="RepoContextRetrievalReadinessState.Phase"/> answers "can this host
/// serve semantic retrieval", and an unarmed plane answers that question with a
/// truthful yes: an exhaustive scan over the vectors the index holds returns
/// complete recall, so it is slower and never worse. Arming answers a different
/// question - "is the approximate index doing the job it exists to do" - and the
/// two have different correct answers on the same box. Folding arming into the
/// readiness verdict would fail a deployment whose corpus is simply too small to
/// partition, which is a legitimate steady state and not a fault.
/// </para>
/// <para>
/// <b>Why it is reported at all.</b> Before this existed, every readiness and
/// retrieval-path surface produced the same value for both cases: the
/// caller-facing declaration is
/// <see cref="RepoContextRetrievalPath.SemanticApproximate"/> per <i>index</i>
/// rather than per query (deliberately, and see
/// <see cref="AnnRepoContextSemanticIndex"/> for why that must not change), so an
/// unarmed plane and an armed one were indistinguishable from outside the
/// process. The distinction was computed per query as
/// <see cref="RepoContextAnnServingState"/> and published on the
/// <c>repocontext.retrieval.ann.search</c> instrument, but that instrument needs
/// query traffic to say anything, and readiness - the surface documented as
/// covering the no-traffic case - had never been given the condition to report.
/// </para>
/// </summary>
public enum RepoContextRetrievalArming
{
    /// <summary>
    /// Nothing has yet observed which path inside the plane answered a query, so
    /// arming is unknown. This is the honest reading before the first query the
    /// plane answers for itself, and it is deliberately distinct from
    /// <see cref="Unarmed"/>: "not yet observed" and "observed to be unarmed" are
    /// different facts, and conflating them would reproduce the exact defect this
    /// type exists to remove.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// The plane last answered by exhaustive scan of the vectors it holds, so it
    /// holds no usable partitioning: its corpus is below the training threshold,
    /// or training has not run. Recall over the indexed corpus is complete, so
    /// this is a warming or a small-corpus plane, never a degraded one.
    /// </summary>
    Unarmed = 1,

    /// <summary>
    /// The plane last answered from its trained partitioning. This is the steady
    /// state the approximate index exists to reach.
    /// </summary>
    Armed = 2,
}
