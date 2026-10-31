using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>What one checkpoint captures beyond its defaults.</summary>
public sealed class SubstepOptions
{
    /// <summary>Further regions to mask in the screenshot, beyond <see cref="JourneyMasks.Default"/>.</summary>
    public IReadOnlyList<ILocator> Mask { get; init; } = [];

    /// <summary>
    /// Whether to take the screenshot of the whole scrollable page (the default) or only the
    /// viewport, for a state whose point is what is in view.
    /// </summary>
    public bool FullPage { get; init; } = true;
}
