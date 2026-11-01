using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// What one caller may do in the Apps area, probed per circuit and re-read in the
/// background once it is older than the chrome's capability refresh interval: the advisory
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

    /// <summary>
    /// The installed apps still present in the tenant - every one but an uninstalled record - in
    /// slug order: what an <c>AppInstall</c> holder's Apps page lists and the spine badge counts.
    /// </summary>
    public ImmutableArray<AppSummary> InTenant =>
        [.. Installed.Where(app => app.State != AppLifecycleState.Uninstalled).OrderBy(app => app.Slug, StringComparer.Ordinal)];

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

    /// <summary>
    /// Whether a capability probe met no answer from the cluster (issue #5420): its
    /// flags still read denied, but the snapshot is not remembered, and a snapshot
    /// that shows nothing reads as not checked rather than as a refusal.
    /// </summary>
    public bool ProbeUnanswered { get; init; }

    /// <summary>The installed apps whose last activation is known to have failed.</summary>
    public IEnumerable<AppSummary> FailedActivations => Installed.Where(app => app.State == AppLifecycleState.Failed);

    /// <summary>
    /// Whether <paramref name="other"/> shows the caller the same thing as this snapshot:
    /// the same flags and the same apps, roles, versions, states and updates. A background
    /// re-read that answers the same is not announced, so an unchanged estate costs no
    /// re-render and no second directory pass (issue #5517).
    /// </summary>
    /// <param name="other">The snapshot to compare with.</param>
    public bool ShowsSameAs(AppsAccessSnapshot other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Catalog == other.Catalog
            && Control == other.Control
            && WorkspaceServed == other.WorkspaceServed
            && ProbeUnanswered == other.ProbeUnanswered
            && Same(MyApps, other.MyApps, static (a, b) =>
                a.Slug == b.Slug
                && a.Version == b.Version
                && a.InstallRevision == b.InstallRevision
                && a.HasUi == b.HasUi
                && a.Roles.AsSpan().SequenceEqual(b.Roles.AsSpan()))
            && Same(Installed, other.Installed, static (a, b) =>
                a.Slug == b.Slug
                && a.Version == b.Version
                && a.State == b.State
                && a.Provenance.Source == b.Provenance.Source)
            && Same(Updates, other.Updates, static (a, b) =>
                a.SourceKey == b.SourceKey
                && a.Slug == b.Slug
                && a.NewestVersion == b.NewestVersion
                && a.InstalledVersion == b.InstalledVersion
                && a.InstalledState == b.InstalledState);
    }

    private static bool Same<T>(ImmutableArray<T> left, ImmutableArray<T> right, Func<T, T, bool> equal)
    {
        left = left.IsDefault ? [] : left;
        right = right.IsDefault ? [] : right;
        if (left.Length != right.Length)
        {
            return false;
        }

        for (var i = 0; i < left.Length; i++)
        {
            if (!equal(left[i], right[i]))
            {
                return false;
            }
        }

        return true;
    }
}
