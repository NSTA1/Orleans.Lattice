namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The closed, machine-readable vocabulary describing <b>which retrieval path
/// actually answered</b> a <c>repocontext_search</c> or <c>repocontext_context</c>
/// call, and - when the answer was a keyword scan - <b>why</b> the semantic path
/// did not answer.
/// <para>
/// <b>Why it exists.</b> The long-standing <c>mode</c> field reports only
/// <c>"semantic"</c>, <c>"keyword"</c>, or <c>"empty"</c>. That makes
/// <c>mode: "keyword"</c> ambiguous: it is the documented graceful fallback for a
/// correctly-configured box with no embedding provider bound, and it is also what a
/// caller sees when the vector plane is unavailable, still building, or degraded. A
/// real capability loss is therefore indistinguishable from an intended keyword-only
/// deployment, so nothing alerts. This vocabulary separates the two causes.
/// </para>
/// <para>
/// <b>Additive, never breaking.</b> The retrieval path rides <i>alongside</i>
/// <c>mode</c>; <c>mode</c> keeps its existing values and meaning exactly, so a client
/// that only reads <c>mode</c> is unaffected. A <c>semantic.*</c> path always
/// accompanies <c>mode: "semantic"</c>; a <c>keyword.*</c> path accompanies
/// <c>mode: "keyword"</c> or <c>mode: "empty"</c> (an empty result still carries the
/// cause the semantic path was skipped for).
/// </para>
/// <para>
/// <b>Server-derived only.</b> Every value is resolved from authoritative local state
/// on the retrieval path itself. No part of it is ever taken from a caller argument or
/// any other wire-supplied text, and a declaration from a host-bound index seam is
/// re-validated locally through <see cref="NormalizeSemantic(string?)"/> before it is
/// reported.
/// </para>
/// </summary>
public static class RepoContextRetrievalPath
{
    /// <summary>
    /// Wire value <c>"semantic.exact"</c>: an <b>exact</b> nearest-neighbour search
    /// answered, so recall is complete - every stored vector in the query's embedding
    /// space was considered. Emitted only when the bound semantic index explicitly
    /// declares exact search and it produced at least one hydrated hit.
    /// </summary>
    public const string SemanticExact = "semantic.exact";

    /// <summary>
    /// Wire value <c>"semantic.approximate"</c>: an <b>approximate</b>
    /// nearest-neighbour search answered, so recall is bounded rather than complete -
    /// a close match may have been missed in exchange for a bounded query cost.
    /// Emitted when the bound semantic index declares approximate search (or declares
    /// nothing recognisable - see <see cref="NormalizeSemantic(string?)"/>) and it
    /// produced at least one hydrated hit.
    /// </summary>
    public const string SemanticApproximate = "semantic.approximate";

    /// <summary>
    /// Wire value <c>"keyword.no_embedder"</c>: the keyword/structural scan answered
    /// because <b>no embedding provider is bound</b> at all. This is a correctly
    /// configured keyword-only deployment in its intended steady state, not a fault,
    /// and it is the one keyword cause that still reports the host ready.
    /// </summary>
    public const string KeywordNoEmbedder = "keyword.no_embedder";

    /// <summary>
    /// Wire value <c>"keyword.vector_plane_unavailable"</c>: an embedding provider is
    /// bound but the vector plane <b>could not serve the query</b> - the provider was
    /// unreachable, the query embedding did not succeed, or the index holds no vectors
    /// in the query's embedding space yet because the plane is still building
    /// (cold start, WAL replay, or a re-derivation back-fill in flight). This is a
    /// real capability loss, distinct from <see cref="KeywordNoEmbedder"/>.
    /// </summary>
    public const string KeywordVectorPlaneUnavailable = "keyword.vector_plane_unavailable";

    /// <summary>
    /// Wire value <c>"keyword.index_degraded"</c>: an embedding provider is bound and
    /// the semantic path ran, but the <b>semantic index itself is degraded</b> - it
    /// threw (an index or backing-projection fault the fail-closed guard caught), or it
    /// ranked candidates that no longer hydrate from the store of record. This is a
    /// real capability loss, distinct from <see cref="KeywordNoEmbedder"/>.
    /// </summary>
    public const string KeywordIndexDegraded = "keyword.index_degraded";

    /// <summary>
    /// Re-validates a semantic-path declaration from a host-bound
    /// <c>IRepoContextSemanticIndex</c> against this local vocabulary, so an index
    /// implementation can never put arbitrary text on a response.
    /// <para>
    /// <b>Fail closed by under-promising.</b> Only the exact literal
    /// <see cref="SemanticExact"/> yields <see cref="SemanticExact"/>. Anything else -
    /// <see langword="null"/>, empty, or an unrecognised value - resolves to
    /// <see cref="SemanticApproximate"/>, the weaker recall claim, because
    /// over-reporting recall would let a caller trust a completeness guarantee the
    /// index never made.
    /// </para>
    /// </summary>
    /// <param name="declared">The value the bound index declared, which may be <see langword="null"/>.</param>
    /// <returns><see cref="SemanticExact"/> only for an exact declaration; otherwise <see cref="SemanticApproximate"/>.</returns>
    public static string NormalizeSemantic(string? declared)
        => string.Equals(declared, SemanticExact, StringComparison.Ordinal)
            ? SemanticExact
            : SemanticApproximate;

    /// <summary>
    /// Whether <paramref name="path"/> is one of the two semantic values, so the
    /// vector plane demonstrably answered the query.
    /// </summary>
    /// <param name="path">A retrieval-path value, which may be <see langword="null"/>.</param>
    /// <returns><see langword="true"/> for <see cref="SemanticExact"/> or <see cref="SemanticApproximate"/>; otherwise <see langword="false"/>.</returns>
    public static bool IsSemantic(string? path)
        => string.Equals(path, SemanticExact, StringComparison.Ordinal)
            || string.Equals(path, SemanticApproximate, StringComparison.Ordinal);
}
