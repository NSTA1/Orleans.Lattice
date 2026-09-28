using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>A navigation manager outside a renderer that records where it was sent.</summary>
internal sealed class TestNavigationManager : NavigationManager
{
    /// <summary>Creates a manager at <paramref name="relative"/> under <c>https://host/explorer/</c>.</summary>
    /// <param name="relative">The starting base-relative location.</param>
    public TestNavigationManager(string relative = "")
    {
        Initialize("https://host/explorer/", "https://host/explorer/" + relative);
    }

    /// <summary>Every navigation requested, as the relative target and whether it replaced history.</summary>
    public List<(string Uri, bool Replace)> Navigations { get; } = [];

    /// <inheritdoc />
    protected override void NavigateToCore(string uri, NavigationOptions options)
    {
        Navigations.Add((uri, options.ReplaceHistoryEntry));
        // Kept as text, as a browser keeps it: System.Uri would decode an escaped
        // unreserved character such as %4F and lose the canonical form.
        Uri = uri.Contains("://", StringComparison.Ordinal) ? uri : BaseUri + uri.TrimStart('/');
    }
}
