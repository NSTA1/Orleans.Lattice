namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The persisted intent of one approximate-index build coordinator, stored under
/// the grain's key - the repository and the embedding-space fingerprint. It
/// records the space the build covers, whether the index has converged, and
/// whether the superseded sibling spaces have been reclaimed.
/// <para>
/// <b>Every value-typed member's "off" state is the CLR type default, and that is
/// a correctness requirement rather than a style choice.</b> The grain-storage
/// serializer omits any member equal to <c>default(T)</c>, so a non-default
/// initializer would let an omitted member reconstruct as the initializer's value
/// instead of the value that was written. Here both flags default to
/// <see langword="false"/>, which is also the safe reading: an omitted flag says
/// "not converged" and "not reclaimed", so the coordinator re-drives work that is
/// idempotent rather than skipping work that was never done.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.RepoContextAnnIndexBuildState)]
internal sealed class RepoContextAnnIndexBuildState
{
    /// <summary>
    /// The embedding space this coordinator's index covers. Persisted rather than
    /// re-derived because the grain key carries only the space's fingerprint, and a
    /// fingerprint is one-way: a reminder-driven reactivation must be able to build
    /// without an embedding provider being resolvable on the silo it lands on.
    /// A default (unspecified) tag means intent was never persisted, so there is
    /// nothing to build.
    /// </summary>
    [Id(0)]
    public EmbeddingSpaceTag Space { get; set; }

    /// <summary>
    /// Whether the index reached <c>Ready</c>. False - the type default, and so
    /// also what an omitted member reconstructs as - means the coordinator still
    /// has work outstanding and keeps its keep-alive reminder registered.
    /// </summary>
    [Id(1)]
    public bool Converged { get; set; }

    /// <summary>
    /// Whether the superseded sibling space prefixes for this repository have been
    /// retired. Set only after <see cref="Converged"/>, so a re-embed that failed
    /// part way can still fall back to the previous space's index. False means
    /// "retry the reclamation", which is idempotent.
    /// </summary>
    [Id(2)]
    public bool Reclaimed { get; set; }

    /// <summary>
    /// How many vectors the index reported holding when it converged. Recorded for
    /// diagnostics only; nothing reads it to make a decision, so a zero (whether
    /// written or omitted) is never mistaken for a signal.
    /// </summary>
    [Id(3)]
    public long VectorsIndexed { get; set; }
}
