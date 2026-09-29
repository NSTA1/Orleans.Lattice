using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// The Replication area's section links: the estate and the enrolled trees, with the
/// current section marked and the active filters carried across.
/// </summary>
public partial class ReplicationSections
{
    /// <summary>Which section an address belongs to.</summary>
    internal enum SectionKind
    {
        /// <summary>The estate view.</summary>
        Estate,

        /// <summary>The enrolled-trees list.</summary>
        Trees,

        /// <summary>One tree's detail, under the enrolled trees.</summary>
        Tree,
    }

    [CascadingParameter]
    internal ExplorerLocation? Location { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    internal SectionKind Section => Current.Path.Count switch
    {
        0 => SectionKind.Estate,
        1 => SectionKind.Trees,
        _ => SectionKind.Tree,
    };

    private ExplorerAddress Current => Location?.Address ?? Navigator.Current ?? ReplicationAddresses.Estate;

    private string? TreesCurrent => Section switch
    {
        SectionKind.Trees => "page",
        SectionKind.Tree => "location",
        _ => null,
    };

    private string Href(ExplorerAddress section)
    {
        var target = section;
        foreach (var key in (string[])[ReplicationAddresses.HealthQuery, ReplicationAddresses.RegionQuery, ReplicationAddresses.AppQuery])
        {
            if (Current.GetQuery(key) is { Length: > 0 } value)
            {
                target = target.WithQuery(key, value);
            }
        }

        return Navigator.Canonicalize(target).ToHref();
    }
}
