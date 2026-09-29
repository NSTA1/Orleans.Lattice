using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The Access area's navigation between its rules, groups and explain pages.
/// The explain link is the visible control of the "Explain access..." command.
/// </summary>
public partial class AccessNav
{
    /// <summary>The current section: <c>rules</c>, <c>groups</c> or <c>explain</c>.</summary>
    [Parameter]
    public string? Current { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private string? CurrentFor(string section) =>
        string.Equals(Current, section, StringComparison.Ordinal) ? "page" : null;
}
