namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_context</c> tool: a ranked, explained bundle of
/// source packed for one natural-language task under a <b>hard</b> token ceiling, so
/// an agent can collapse the search -> recall -> read loop into a single round trip
/// and never overrun its context budget.
/// <para>
/// <b>The ceiling is never exceeded.</b> <see cref="ResponseTokens"/> is the estimated
/// cost of the response as the caller receives it - delivered content plus each entry's
/// JSON envelope, times the SDK's dual-emission factor - and is always less than or
/// equal to <see cref="BudgetTokens"/>. <see cref="TotalTokens"/> reports the narrower
/// figure of the packed source text alone. When even the cheapest single entry does not
/// fit the budget, the bundle fails closed: <see cref="Entries"/> is empty and
/// <see cref="RetryBudgetTokens"/> reports a budget that is guaranteed to fit at
/// least one entry on a retry. An empty <see cref="Entries"/> with a
/// <see langword="null"/> <see cref="RetryBudgetTokens"/> means the search matched
/// nothing, so no larger budget would help.
/// </para>
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextContextResult
{
    /// <summary>The repository the task ran against.</summary>
    public required string RepoId { get; init; }

    /// <summary>The original natural-language task the bundle was packed for.</summary>
    public required string Task { get; init; }

    /// <summary>
    /// How the underlying search produced its hits: <c>"semantic"</c> (the vector
    /// index answered), <c>"keyword"</c> (a degraded structural token scan), or
    /// <c>"empty"</c> (no match). A keyword bundle is still a valid bundle.
    /// </summary>
    public required string Mode { get; init; }

    /// <summary>
    /// The precise retrieval path the underlying search took, from the closed
    /// <see cref="RepoContextRetrievalPath"/> vocabulary. It rides <b>alongside</b>
    /// <see cref="Mode"/> without changing it, and disambiguates what <see cref="Mode"/>
    /// cannot: whether a semantic answer had complete or bounded recall, and - when the
    /// answer was a keyword scan - whether that is an intended keyword-only deployment
    /// (<see cref="RepoContextRetrievalPath.KeywordNoEmbedder"/>) or a real capability
    /// loss (<see cref="RepoContextRetrievalPath.KeywordVectorPlaneUnavailable"/> or
    /// <see cref="RepoContextRetrievalPath.KeywordIndexDegraded"/>). Always server-derived
    /// and never <see langword="null"/>.
    /// </summary>
    public required string RetrievalPath { get; init; }

    /// <summary>
    /// The concrete detail level the bundle was packed at - <c>"paths"</c>,
    /// <c>"outline"</c>, or <c>"slices"</c> - after resolving an
    /// <see cref="RepoContextContextDetail.Auto"/> request. Never <c>"auto"</c>.
    /// </summary>
    public required string Detail { get; init; }

    /// <summary>The hard token ceiling the bundle was packed under, after clamping the requested budget.</summary>
    public required int BudgetTokens { get; init; }

    /// <summary>
    /// The exact BPE token sum of the source text delivered in <see cref="Entries"/>,
    /// excluding the JSON envelope around it. This is the "how much source did I get"
    /// figure; <see cref="ResponseTokens"/> is the "what did this cost me" figure, and
    /// it is the latter that the budget bounds.
    /// </summary>
    public required int TotalTokens { get; init; }

    /// <summary>
    /// The estimated token cost of the response as the caller actually receives it:
    /// the delivered content plus each entry's JSON envelope (path, match reasons,
    /// content hash, and per-unit receipts), multiplied by the MCP SDK's dual-emission
    /// factor because every tool result is serialized twice - once as structured
    /// content and once as text. Always less than or equal to
    /// <see cref="BudgetTokens"/>: this is the invariant the tool exists to guarantee.
    /// The estimate is deliberately conservative, so a bundle may come in slightly
    /// under the ceiling but never over it.
    /// </summary>
    public required int ResponseTokens { get; init; }

    /// <summary>
    /// Whether at least one ranked candidate was dropped because it did not fit the
    /// remaining budget, so the bundle is a prefix of the available context rather
    /// than the whole of it.
    /// </summary>
    public required bool Truncated { get; init; }

    /// <summary>
    /// When <see cref="Entries"/> is empty because nothing fit the budget, a token
    /// budget that is guaranteed to admit at least one entry on a retry (the cost of
    /// the cheapest candidate at the resolved detail level); otherwise
    /// <see langword="null"/>. A <see langword="null"/> value alongside an empty
    /// <see cref="Entries"/> means the search matched nothing, so no larger budget
    /// would help.
    /// </summary>
    public required int? RetryBudgetTokens { get; init; }

    /// <summary>
    /// The packed files in descending search-score order, each with its packed
    /// content, token cost, and match reasons; empty when nothing fit the budget or
    /// the search matched nothing.
    /// </summary>
    public required IReadOnlyList<RepoContextContextEntry> Entries { get; init; }

    /// <summary>
    /// The caller session this bundle's reuse bookkeeping was recorded under, echoed
    /// back so the caller can carry it on the next call; <see langword="null"/> when no
    /// session was supplied and no cross-call reuse state was persisted.
    /// </summary>
    public string? Session { get; init; }

    /// <summary>
    /// The reuse acknowledgements for content the tool suppressed instead of
    /// re-delivering, because the caller already held it (through a <c>seen</c> receipt,
    /// a validated <c>known</c> possession claim, or the named <c>session</c>'s recorded
    /// history). Never counted against <see cref="TotalTokens"/> or the file budget.
    /// Empty when nothing was suppressed. Never <see langword="null"/>.
    /// </summary>
    public IReadOnlyList<RepoContextReuseAck> Reused { get; init; } = Array.Empty<RepoContextReuseAck>();
}
