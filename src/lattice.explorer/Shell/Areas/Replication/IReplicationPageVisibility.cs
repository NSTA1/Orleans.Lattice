namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// Whether the document the circuit renders into is visible, so a refresh cadence
/// can stop while the tab is in the background (the Page Visibility API).
/// </summary>
internal interface IReplicationPageVisibility
{
    /// <summary>Whether the page is visible. Reads <see langword="true"/> until observation says otherwise.</summary>
    bool IsVisible { get; }

    /// <summary>Raised when <see cref="IsVisible"/> changes. May be raised off the renderer's thread.</summary>
    event Action? Changed;

    /// <summary>Starts observing, once per circuit; later calls do nothing. Never throws.</summary>
    ValueTask StartAsync();
}
