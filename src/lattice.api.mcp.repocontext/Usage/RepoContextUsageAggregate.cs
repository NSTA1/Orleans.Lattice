namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// An aggregate roll-up of recorded <see cref="RepoContextCallUsage"/> figures over a bounded
/// window: how many calls were answered, the total response tokens spent, and the total
/// whole-file read tokens replaced. Carries only summed token figures - never bodies, queries, or paths.
/// </summary>
/// <param name="Calls">The number of answered calls counted in the window.</param>
/// <param name="ResponseTokens">The total exact response tokens spent across those calls.</param>
/// <param name="ReadsReplacedTokens">The total conservatively credited whole-file read tokens those calls replaced.</param>
internal readonly record struct RepoContextUsageAggregate(long Calls, long ResponseTokens, long ReadsReplacedTokens)
{
    /// <summary>
    /// The net tokens saved across the window: the read tokens replaced minus the response tokens spent.
    /// </summary>
    public long NetSavedTokens => ReadsReplacedTokens - ResponseTokens;
}
