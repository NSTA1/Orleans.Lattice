using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The Access area: rules, groups, and the explanation of an access decision,
/// bound to <see cref="ILatticeAuthAdmin"/>. The policy store is cluster-wide, so
/// its plain addresses are too; a tenant-rooted address (<c>/t/{tenant}/access</c>)
/// shows only the rules that govern that tenant's own trees.
/// </summary>
/// <remarks>
/// <para>
/// Visibility comes from a fail-closed probe: the smallest page of the group
/// catalogue, which the facade admits only for an administrator. It succeeds, and
/// the area is visible. It is refused while the Explorer is not signed in, and the
/// area stays in the directory as unavailable with a sign-in sentence, so an
/// anonymous circuit reads as "sign in", never as an empty cluster. Any other
/// outcome - a signed-in identity without the grant, a cluster that does not serve
/// the facade, no connection - hides the area, unless the second question below
/// admits it.
/// </para>
/// <para>
/// That second question is for a tenant administrator who is not a cluster one:
/// the tenant posture probe for the circuit's asserted tenant. The area is visible
/// only when it reports delegated tenant access administration enabled and the
/// caller an admin of that tenant or a platform operator. Off, not permitted, the
/// reserved default tenant, no asserted tenant, or no answer all keep it hidden,
/// so nothing is widened for anyone else.
/// </para>
/// <para>
/// The verdict is memoised per circuit for the caller it was reached for (the
/// sign-in, the endpoint and the asserted tenant), so the directory's
/// per-navigation ask is free until any of them changes.
/// </para>
/// </remarks>
internal sealed class AccessArea : IExplorerArea, IPlatformOperatorProbe
{
    /// <summary>The reason shown while the Explorer is not signed in.</summary>
    public const string SignInReason = "Sign in to administer access on this cluster.";

    private static readonly AuthPageRequest ProbePage = new() { PageSize = 1 };

    private readonly ILatticeAuthAdmin? _admin;
    private readonly AccessCatalog? _catalog;
    private readonly TenantAccessCatalog _tenantAccess;
    private readonly ShellAssertedTenant _tenant;
    private readonly ShellCaller _caller;
    private (ShellCallerKey Caller, AreaAvailability Availability)? _verdict;
    private (ShellCallerKey Caller, AreaAvailability Availability)? _clusterVerdict;

    /// <summary>Creates the area over the circuit's services, any of which may be absent.</summary>
    /// <param name="services">The circuit's services.</param>
    public AccessArea(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _admin = services.GetShellFacade<ILatticeAuthAdmin>();
        _tenant = services.GetService<ShellAssertedTenant>() ?? ShellAssertedTenant.None;
        _caller = ShellCaller.Of(services);
        _catalog = _admin is null ? null : services.GetService<AccessCatalog>() ?? new AccessCatalog(_admin, _tenant, _caller);
        _tenantAccess = services.GetService<TenantAccessCatalog>()
            ?? new TenantAccessCatalog(services.GetService<ITenantAccessFacades>() ?? new ShellTenantAccessFacades(services), _caller);
        Completions = _catalog is null ? null : new AccessCompletionSource(_catalog, _tenantAccess);
        Commands =
        [
            new ExplorerCommand(ExplainCommandId, "Explain access...")
            {
                Target = AccessRoutes.Explain,
                Detail = "Why a subject is allowed or denied an operation, and its effective permissions",
            },
            new ExplorerCommand(CreateRuleCommandId, "Create an access rule")
            {
                Target = AccessRoutes.Rules.WithQuery(AccessRoutes.NewQuery, "true"),
            },
            new ExplorerCommand(CreateGroupCommandId, "Create a group")
            {
                Target = AccessRoutes.Groups.WithQuery(AccessRoutes.NewQuery, "true"),
            },
        ];
    }

    /// <summary>The id of the "Explain access..." command.</summary>
    public const string ExplainCommandId = "access.explain";

    /// <summary>The id of the "Create an access rule" command.</summary>
    public const string CreateRuleCommandId = "access.create-rule";

    /// <summary>The id of the "Create a group" command.</summary>
    public const string CreateGroupCommandId = "access.create-group";

    /// <inheritdoc />
    public string Key => AccessRoutes.AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Access";

    /// <inheritdoc />
    public int DirectoryOrder => 30;

    /// <inheritdoc />
    public bool IsTenantScoped => false;

    /// <summary>
    /// Whether <paramref name="address"/> follows the active tenant: it does when
    /// it carries a tenant root, and then shows only that tenant's rules. The
    /// plain <c>/access</c> addresses stay cluster-wide, unchanged.
    /// </summary>
    /// <param name="address">An address in this area.</param>
    public bool IsTenantScopedAt(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return address.Tenant is not null;
    }

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; }

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        if (_admin is null && !_tenantAccess.IsServed)
        {
            return AreaAvailability.Hidden;
        }

        var caller = _caller.Current;
        if (_verdict is { } memo && memo.Caller == caller)
        {
            return memo.Availability;
        }

        var availability = await ProbeAsync(caller.Authenticated, cancellationToken).ConfigureAwait(true);
        if (_caller.Current == caller)
        {
            _verdict = (caller, availability);
        }

        return availability;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        if (_catalog is null)
        {
            return null;
        }

        var model = await _catalog.GetAccessModelAsync(cancellationToken).ConfigureAwait(true);
        return model is null ? null
            : model.RulesEnforced ? "Rules are enforced."
            : "Rules are recorded but not enforced.";
    }

    private async Task<AreaAvailability> ProbeAsync(bool authenticated, CancellationToken cancellationToken)
    {
        var cluster = await GetClusterAvailabilityAsync(authenticated, cancellationToken).ConfigureAwait(true);
        if (cluster.Kind != AreaAvailabilityKind.Hidden)
        {
            return cluster;
        }

        // Not a cluster access administrator. A tenant admin of the active tenant
        // still sees the area while the cluster has delegated tenant access
        // administration on; nobody else is let in by this second question.
        return await ProbeTenantAsync(cancellationToken).ConfigureAwait(true);
    }

    /// <summary>
    /// Whether the caller administers access for the whole cluster: the cluster
    /// probe alone, never the tenant posture question that also admits a delegated
    /// tenant administrator to the area. The platform-operator gate asks this, so a
    /// tenant administrator never gains operator standing - the tenant switcher or
    /// the reserved default tenant - by seeing the area.
    /// </summary>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><see langword="true"/> only when the cluster admitted the probe.</returns>
    public async ValueTask<bool> IsClusterAccessAdministratorAsync(CancellationToken cancellationToken)
    {
        var cluster = await GetClusterAvailabilityAsync(_caller.Current.Authenticated, cancellationToken).ConfigureAwait(true);
        return cluster.Kind == AreaAvailabilityKind.Visible;
    }

    private async ValueTask<AreaAvailability> GetClusterAvailabilityAsync(bool authenticated, CancellationToken cancellationToken)
    {
        if (_admin is null)
        {
            return AreaAvailability.Hidden;
        }

        var caller = _caller.Current;
        if (_clusterVerdict is { } memo && memo.Caller == caller)
        {
            return memo.Availability;
        }

        var availability = await ProbeClusterAsync(authenticated, cancellationToken).ConfigureAwait(true);
        if (_caller.Current == caller)
        {
            _clusterVerdict = (caller, availability);
        }

        return availability;
    }

    private async Task<AreaAvailability> ProbeClusterAsync(bool authenticated, CancellationToken cancellationToken)
    {
        try
        {
            var page = await _admin!.ListGroupsAsync(ProbePage, cancellationToken).ConfigureAwait(true);

            // An absent answer proved nothing, so it admits nothing.
            return page is null ? AreaAvailability.Hidden : AreaAvailability.Visible;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return authenticated ? AreaAvailability.Hidden : AreaAvailability.Unavailable(SignInReason);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return AreaAvailability.Hidden;
        }
    }

    private async Task<AreaAvailability> ProbeTenantAsync(CancellationToken cancellationToken)
    {
        var tenant = _tenant.AssertedTenant;
        if (!_tenantAccess.IsServed || !TenantAccessCatalog.Administers(tenant))
        {
            return AreaAvailability.Hidden;
        }

        var state = await _tenantAccess.GetStateAsync(tenant!, cancellationToken).ConfigureAwait(true);
        return state.IsDelegated ? AreaAvailability.Visible : AreaAvailability.Hidden;
    }
}
