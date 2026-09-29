using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster.Pages;

/// <summary>
/// The heading of every page about one tree: its logical id in mono and, beside
/// it, the app and tenant that own it. The physical id is never shown.
/// </summary>
public partial class ClusterTreeHeader
{
    /// <summary>The logical tree id.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>A word before the tree id, such as "Reshard"; the heading is the tree id alone when <see langword="null"/>.</summary>
    [Parameter]
    public string? Heading { get; set; }

    /// <summary>The page's lede, under the heading.</summary>
    [Parameter]
    public RenderFragment? ChildContent { get; set; }
}
