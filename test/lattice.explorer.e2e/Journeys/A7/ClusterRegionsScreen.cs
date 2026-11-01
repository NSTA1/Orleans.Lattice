using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A7;

/// <summary>The Cluster overview's <b>Regions</b> section: this region and each peer it replicates with.</summary>
internal sealed class ClusterRegionsScreen(ShellPage shell)
{
    /// <summary>The chrome.</summary>
    public ShellPage Shell { get; } = shell;

    /// <summary>The overview's <b>Regions</b> section.</summary>
    public ILocator Section => Shell.Content.GetByRole(AriaRole.Region, new() { Name = "Regions", Exact = true });

    /// <summary>The region diagram.</summary>
    public ILocator Diagram => Section.Locator("figure");

    /// <summary>The diagram's peers.</summary>
    public ILocator Peers => Diagram.GetByRole(AriaRole.Listitem);

    /// <summary>
    /// Goes to the Cluster overview from the directory and waits for its region diagram. Regions
    /// belong to the whole cluster, so an overview scoped to a tenant shows only a link to the
    /// cluster-wide overview in their place; that link is followed when it is offered.
    /// </summary>
    public async Task OpenAsync()
    {
        await Shell.GoToAreaAsync("cluster");
        await Shell.ExpectHeadingAsync("Cluster");
        var clusterWide = Shell.Content.GetByRole(AriaRole.Link, new() { Name = "See the cluster-wide overview", Exact = true });
        await Expect(Diagram.Or(clusterWide)).ToBeVisibleAsync(new() { Timeout = 60_000 });
        if (await clusterWide.IsVisibleAsync())
        {
            await clusterWide.ClickAsync();
            await Shell.ExpectHeadingAsync("Cluster");
        }

        await Expect(Diagram).ToBeVisibleAsync(new() { Timeout = 60_000 });
    }
}
