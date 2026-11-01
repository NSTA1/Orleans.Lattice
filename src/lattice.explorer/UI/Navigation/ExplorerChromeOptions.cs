namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The navigation chrome's time bounds: how long the directory waits for an area
/// to answer, and how long the address line waits for a completion source.
/// </summary>
/// <remarks>
/// Every bound fails closed: an area that has not answered in time is not
/// admitted - it is shown as not checked and asked again - a Home status that has
/// not arrived says so, and a completion source that has not answered contributes
/// nothing, so no single slow facade can stall the chrome.
/// </remarks>
internal sealed class ExplorerChromeOptions
{
    /// <summary>How long the directory waits for an area's availability. Defaults to three seconds.</summary>
    public TimeSpan AvailabilityTimeout { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>How long Home waits for an area's one-line status. Defaults to three seconds.</summary>
    public TimeSpan HomeStatusTimeout { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>How long the address line waits for one completion source. Defaults to two seconds.</summary>
    public TimeSpan CompletionTimeout { get; init; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// How long an area trusts its memoized probe of the caller's rights and apps
    /// before the next read re-reads it in the background (issue #5517). A grant, a
    /// role binding or a group membership changed elsewhere therefore reaches an open
    /// console on the first navigation after this interval, without a reload, while
    /// a read never waits for the re-read and no render probes on its own. Defaults
    /// to ten seconds.
    /// </summary>
    public TimeSpan CapabilityRefreshInterval { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long the directory waits before each automatic recheck of an area, or an
    /// area's Home status, that could not be checked because the cluster did not
    /// answer: one delay per attempt, after which it stops and leaves the manual
    /// retry. Defaults to 2, 4, 8, 16, 30 and 60 seconds.
    /// </summary>
    public IReadOnlyList<TimeSpan> RecheckDelays { get; init; } =
    [
        TimeSpan.FromSeconds(2),
        TimeSpan.FromSeconds(4),
        TimeSpan.FromSeconds(8),
        TimeSpan.FromSeconds(16),
        TimeSpan.FromSeconds(30),
        TimeSpan.FromSeconds(60),
    ];
}
