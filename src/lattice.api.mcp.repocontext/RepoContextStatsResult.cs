namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The structured result of the read-only <c>repocontext_stats</c> tool: an aggregate roll-up of the
/// repository-context surface's usage over a bounded recent window, so a team can see whether the
/// surface actually reduces context cost. It reports only summed token figures - how many calls were
/// answered, the exact response tokens they spent, the whole-file read tokens they conservatively
/// replaced, and the net tokens saved - and never any body text, query, path, or repository identity.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans grain message, so it
/// carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextStatsResult
{
    /// <summary>The number of successfully answered calls counted in the window.</summary>
    public required long Calls { get; init; }

    /// <summary>The total exact response tokens spent across those calls.</summary>
    public required long ResponseTokens { get; init; }

    /// <summary>
    /// The total whole-file read tokens those calls conservatively replaced - credited only for
    /// delivered whole-file-equivalent content, never for discovery, partial detail, or reused content.
    /// </summary>
    public required long ReadsReplacedTokens { get; init; }

    /// <summary>
    /// The net tokens saved across the window: <see cref="ReadsReplacedTokens"/> minus
    /// <see cref="ResponseTokens"/>. Negative when responses cost more than the reads they replaced.
    /// </summary>
    public required long NetSavedTokens { get; init; }

    /// <summary>The length of the aggregation window, in seconds.</summary>
    public required double WindowSeconds { get; init; }

    /// <summary>
    /// Projects a windowed usage aggregate into the read-only stats payload. The net saved figure is
    /// taken from the aggregate, and the window is reported in seconds.
    /// </summary>
    /// <param name="aggregate">The windowed usage aggregate to project.</param>
    /// <param name="window">The length of the aggregation window.</param>
    /// <returns>The stats payload.</returns>
    internal static RepoContextStatsResult From(in RepoContextUsageAggregate aggregate, TimeSpan window)
        => new()
        {
            Calls = aggregate.Calls,
            ResponseTokens = aggregate.ResponseTokens,
            ReadsReplacedTokens = aggregate.ReadsReplacedTokens,
            NetSavedTokens = aggregate.NetSavedTokens,
            WindowSeconds = window.TotalSeconds,
        };
}
