namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The navigation chrome's time bounds: how long the directory waits for an area
/// to answer, and how long the address line waits for a completion source.
/// </summary>
/// <remarks>
/// Every bound fails closed: an area that has not answered in time is hidden, a
/// Home status that has not arrived is omitted, and a completion source that has
/// not answered contributes nothing, so no single slow facade can stall the chrome.
/// </remarks>
internal sealed class ExplorerChromeOptions
{
    /// <summary>How long the directory waits for an area's availability. Defaults to three seconds.</summary>
    public TimeSpan AvailabilityTimeout { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>How long Home waits for an area's one-line status. Defaults to three seconds.</summary>
    public TimeSpan HomeStatusTimeout { get; init; } = TimeSpan.FromSeconds(3);

    /// <summary>How long the address line waits for one completion source. Defaults to two seconds.</summary>
    public TimeSpan CompletionTimeout { get; init; } = TimeSpan.FromSeconds(2);
}
