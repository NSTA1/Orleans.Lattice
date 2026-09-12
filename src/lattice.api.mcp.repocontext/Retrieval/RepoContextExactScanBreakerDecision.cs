namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What <see cref="RepoContextExactScanBreaker"/> concluded about starting an
/// exact gather for one query. The three values are kept apart because they are
/// three different statements about the same repository, and collapsing them to a
/// boolean is what made the breaker's open state indistinguishable from a
/// permanent one - see issue #2362.
/// </summary>
internal enum RepoContextExactScanBreakerDecision
{
    /// <summary>
    /// No stall is on record, so the gather runs exactly as it would with no
    /// breaker present.
    /// </summary>
    Closed = 0,

    /// <summary>
    /// A stall is on record and its retry delay has not elapsed, so the gather is
    /// suppressed and keyword recall serves.
    /// </summary>
    Open = 1,

    /// <summary>
    /// A stall is on record, its retry delay has elapsed, and this caller has been
    /// granted the single half-open probe for the window. The gather runs; if it
    /// completes, the breaker closes without the approximate plane ever having
    /// served. This is the value that makes the open state exitable.
    /// </summary>
    Probe = 2,
}
