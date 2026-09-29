using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Cluster;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Suggestions;

/// <summary>
/// The circuit's suggestion sources for the pickers every area shares: trees,
/// the Cluster area's trees, regions, tenants, users and groups. One per circuit, so what a source
/// remembers is shared by every field on every page and belongs to this circuit
/// alone; each source keys what it remembers on the asserted tenant.
/// </summary>
/// <remarks>
/// Sources are built on first use from the circuit's services, and a dependency
/// the head does not register yields a source that answers unavailable, so a
/// picker degrades to free text rather than failing a render.
/// </remarks>
/// <param name="services">The circuit's services.</param>
internal sealed class ExplorerSuggestions(IServiceProvider services)
{
    private ILtSuggestionSource? _trees;
    private ILtSuggestionSource? _clusterTrees;
    private ILtSuggestionSource? _regions;
    private ILtSuggestionSource? _tenants;
    private ILtSuggestionSource? _users;
    private ILtSuggestionSource? _groups;
    private ILtSuggestionSource? _groupsOrStored;
    private ILtSuggestionSource? _principals;

    /// <summary>The trees the caller can reach, by logical id.</summary>
    public ILtSuggestionSource Trees => _trees ??= Build(() => new TreeSuggestionSource(
        services.GetRequiredService<DataDirectory>(),
        services.GetRequiredService<ExplorerTenancy>()));

    /// <summary>The trees the Cluster area administers, by the logical id its facades take.</summary>
    public ILtSuggestionSource ClusterTrees => _clusterTrees ??= Build(() => new ClusterTreeSuggestionSource(
        services.GetRequiredService<ClusterTreeCatalog>()));

    /// <summary>The cluster's own region and its peer regions.</summary>
    public ILtSuggestionSource Regions => _regions ??= Build(() => new RegionSuggestionSource(
        services.GetShellFacade<ILatticeReplicationStatus>(),
        services.GetService<ShellAssertedTenant>(),
        services.GetService<TimeProvider>()));

    /// <summary>The tenants the caller may reach.</summary>
    public ILtSuggestionSource Tenants => _tenants ??= Build(() => new TenantSuggestionSource(
        services.GetRequiredService<ExplorerTenancy>(),
        services.GetService<ShellAssertedTenant>(),
        services.GetService<TimeProvider>()));

    /// <summary>Users from the identity directory.</summary>
    public ILtSuggestionSource Users => _users ??= Build(() => new DirectorySuggestionSource(Auth(), DirectoryPrincipalKind.User));

    /// <summary>Groups from the identity directory.</summary>
    public ILtSuggestionSource Groups => _groups ??= Build(() => new DirectorySuggestionSource(Auth(), DirectoryPrincipalKind.Group));

    /// <summary>Groups from the identity directory, or from the auth store when there is no directory.</summary>
    public ILtSuggestionSource GroupsOrStoredGroups => _groupsOrStored ??= Build(() => new DirectorySuggestionSource(Auth(), DirectoryPrincipalKind.Group, listStoredGroups: true));

    /// <summary>Users and groups alike from the identity directory, for a field that takes either.</summary>
    public ILtSuggestionSource Principals => _principals ??= Build(() => new DirectorySuggestionSource(Auth(), kind: null));

    /// <summary>The directory source for a rule or membership subject of <paramref name="kind"/>.</summary>
    /// <param name="kind">Whether the subject is a user or a group.</param>
    /// <returns>The source.</returns>
    public ILtSuggestionSource Subjects(LatticeSubjectSelectorKind kind) =>
        kind == LatticeSubjectSelectorKind.Group ? Groups : Users;

    private static ILtSuggestionSource Build(Func<ILtSuggestionSource> create)
    {
        try
        {
            return create();
        }
        catch (InvalidOperationException)
        {
            // A dependency the head does not register: the picker is free text.
            return UnavailableSuggestionSource.Instance;
        }
    }

    private ILatticeAuthAdmin? Auth()
    {
        try
        {
            return services.GetShellFacade<ILatticeAuthAdmin>();
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }
}
