namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_context</c> tool: a ranked, explained bundle of
/// source packed for one natural-language task under a <b>hard</b> token ceiling, so
/// an agent can collapse the search -> recall -> read loop into a single round trip
/// and never overrun its context budget.
/// <para>
/// <b>The ceiling is never exceeded.</b> <see cref="TotalTokens"/> is the exact BPE
/// sum of the packed entries and is always less than or equal to
/// <see cref="BudgetTokens"/>. When even the cheapest single entry does not fit the
/// budget, the bundle fails closed: <see cref="Entries"/> is empty and
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
    /// The concrete detail level the bundle was packed at - <c>"paths"</c>,
    /// <c>"outline"</c>, or <c>"slices"</c> - after resolving an
    /// <see cref="RepoContextContextDetail.Auto"/> request. Never <c>"auto"</c>.
    /// </summary>
    public required string Detail { get; init; }

    /// <summary>The hard token ceiling the bundle was packed under, after clamping the requested budget.</summary>
    public required int BudgetTokens { get; init; }

    /// <summary>
    /// The exact BPE token sum of every entry in <see cref="Entries"/>. Always less
    /// than or equal to <see cref="BudgetTokens"/> - the invariant the whole tool
    /// exists to guarantee.
    /// </summary>
    public required int TotalTokens { get; init; }

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
}
