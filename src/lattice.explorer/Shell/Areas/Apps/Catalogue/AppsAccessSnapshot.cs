using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;

/// <summary>
/// What one caller may do in the Apps area, probed once per circuit: the advisory
/// capability flags of the catalogue and the control facade, the caller's own apps,
/// and - for an <c>AppInstall</c> holder - the installed apps and pending updates.
/// </summary>
/// <remarks>
/// Every member fails closed: a probe that threw, or a facade the head does not
/// serve, leaves its flags denied and its lists empty. The flags are advisory; each
/// real operation still authorizes independently on the cluster.
/// </remarks>
internal sealed record AppsAccessSnapshot
{
    /// <summary>A snapshot that grants nothing and lists nothing.</summary>
    public static AppsAccessSnapshot Denied { get; } = new();

    /// <summary>The catalogue's advisory flags.</summary>
    public LatticeAppCatalogCapabilities Catalog { get; init; } = new();

    /// <summary>The control facade's advisory flags.</summary>
    public LatticeAppsCapabilities Control { get; init; } = new();

    /// <summary>Whether the caller's workspace answered, even with no app: every signed-in user sees "Your apps".</summary>
    public bool WorkspaceServed { get; init; }

    /// <summary>The enabled apps in which the caller holds a role, in slug order.</summary>
    public ImmutableArray<WorkspaceAppSummary> MyApps { get; init; } = [];

    /// <summary>The installed apps in the active tenant; empty unless the caller may list them.</summary>
    public ImmutableArray<AppSummary> Installed { get; init; } = [];

    /// <summary>The installed apps a source offers a newer version of; empty unless the caller may browse the catalogue.</summary>
    public ImmutableArray<AvailableAppSummary> Updates { get; init; } = [];

    /// <summary>Whether the caller holds <c>AppInstall</c> as the catalogue reports it, which shows the Catalogue tab.</summary>
    public bool CanBrowseCatalogue => Catalog.CanListSources && Catalog.CanListAvailable;

    /// <summary>Whether the caller may review an app from a source before installing it.</summary>
    public bool CanReview => CanBrowseCatalogue && Catalog.CanDescribeFromSource;

    /// <summary>Whether the caller may install from the catalogue.</summary>
    public bool CanInstall => CanReview && Control.CanInstall;

    /// <summary>Whether the area is shown at all.</summary>
    public bool IsVisible => WorkspaceServed || CanBrowseCatalogue || Control.CanList;

    /// <summary>The installed apps whose last activation is known to have failed.</summary>
    public IEnumerable<AppSummary> FailedActivations => Installed.Where(app => app.State == AppLifecycleState.Failed);
}
