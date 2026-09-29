using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The Tenancy area: the tenant directory and each tenant's administration for a
/// platform operator (<c>/tenancy</c>), and "my tenant" for whoever administers
/// the tenant they are scoped to (<c>/t/{tenant}/tenancy</c>), bound to the six
/// tenant facades.
/// </summary>
/// <remarks>
/// <para>
/// The area does not exist without tenancy: with it off, or without the
/// self-service facade, it is hidden. Otherwise visibility comes from the
/// caller's proven <see cref="TenancyStanding"/>: an operator or an admin of the
/// scoped tenant sees it. A caller the cluster refuses while the Explorer is not
/// signed in sees the stop as unavailable with a sign-in sentence; any other
/// refusal or fault hides it. A caller scoped to the reserved default tenant who
/// is not an operator administers nothing, so sees nothing.
/// </para>
/// <para>
/// The area's two halves are rooted differently, so it decides tenant-rooting
/// per address: an address that carries a tenant root is "my tenant" and follows
/// the active tenant; a plain one is the cluster-wide directory.
/// </para>
/// </remarks>
internal sealed class TenancyArea : IExplorerArea
{
    /// <summary>The reason shown while the Explorer is not signed in.</summary>
    public const string SignInReason = "Sign in to see the tenants you administer.";

    /// <summary>The id of the "Create a tenant" command.</summary>
    public const string CreateTenantCommandId = "tenancy.create-tenant";

    /// <summary>The id of the "Offer a cross-tenant grant" command.</summary>
    public const string OfferGrantCommandId = "tenancy.offer-grant";

    /// <summary>The id of the operator's "Set a tenant's regions" command.</summary>
    public const string SetRegionsCommandId = "tenancy.set-regions";

    /// <summary>The id of the "Change residency" command, for whoever administers the scoped tenant.</summary>
    public const string ChangeResidencyCommandId = "tenancy.change-residency";

    private readonly TenancyCatalog _catalog;
    private (bool Authenticated, string? User, string? Active, AreaAvailability Availability)? _verdict;

    /// <summary>Creates the area over the circuit's services, any of which may be absent.</summary>
    /// <param name="services">The circuit's services.</param>
    public TenancyArea(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _catalog = services.GetService<TenancyCatalog>() ?? new TenancyCatalog(services);
        Completions = _catalog.SelfService is null ? null : new TenancyCompletionSource(_catalog);
    }

    /// <inheritdoc />
    public string Key => TenancyRoutes.AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Tenancy";

    /// <inheritdoc />
    public int DirectoryOrder => 50;

    /// <inheritdoc />
    public bool IsTenantScoped => true;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <summary>
    /// The commands the caller's standing admits: creating a tenant and setting a
    /// tenant's regions for an operator, and changing the scoped tenant's
    /// residency and offering a grant from it for whoever administers it. None
    /// until the standing is proven.
    /// </summary>
    public IReadOnlyList<ExplorerCommand> Commands
    {
        get
        {
            if (_catalog.LastStanding is not { } standing)
            {
                return [];
            }

            var commands = new List<ExplorerCommand>(4);
            if (standing.IsOperator)
            {
                commands.Add(new ExplorerCommand(CreateTenantCommandId, "Create a tenant")
                {
                    Target = TenancyRoutes.Directory.WithQuery(TenancyRoutes.NewQuery, TenancyRoutes.NewValue),
                    Detail = "A new tenant with its first admin subjects",
                });
                commands.Add(new ExplorerCommand(SetRegionsCommandId, "Set a tenant's regions")
                {
                    Target = TenancyRoutes.Directory.WithQuery(TenancyRoutes.SetRegionsQuery, TenancyRoutes.NewValue),
                    Detail = "Choose a tenant, then the regions it is allowed and resident in",
                });
            }

            if (!string.Equals(standing.Workspace, TenantId.DefaultId, StringComparison.Ordinal))
            {
                commands.Add(new ExplorerCommand(ChangeResidencyCommandId, "Change residency")
                {
                    Target = TenancyRoutes.MyTenant(standing.Workspace, TenancyRoutes.RegionsSegment),
                    Detail = $"Where tenant {standing.Workspace}'s data is kept, within its allowed regions",
                });
                commands.Add(new ExplorerCommand(OfferGrantCommandId, "Offer a cross-tenant grant")
                {
                    Target = TenancyRoutes.MyTenant(standing.Workspace, TenancyRoutes.SharingSegment)
                        .WithQuery(TenancyRoutes.NewQuery, TenancyRoutes.NewValue),
                    Detail = $"Share a scope of tenant {standing.Workspace}'s data with another tenant",
                });
            }

            return commands;
        }
    }

    /// <summary>
    /// Whether <paramref name="address"/> follows the active tenant: it does when
    /// it carries a tenant root ("my tenant"), and the plain directory and
    /// administration pages never do.
    /// </summary>
    /// <remarks>
    /// The one exception is the reserved default tenant's workspace root,
    /// <c>/t/default/tenancy</c>. The default tenant has no registry record, so
    /// there is no workspace to show; its root is the tenant directory, which is
    /// what a platform operator scoped there administers. It is therefore not
    /// tenant-rooted and canonicalises to <c>/tenancy</c>, so the spine's stop
    /// leads to the directory from every page. The rule depends on the address
    /// alone, never on a verdict, so it cannot change while one is settling.
    /// </remarks>
    /// <param name="address">An address in this area.</param>
    public bool IsTenantScopedAt(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return address.Tenant is { } tenant
            && !(!address.HasPath && string.Equals(tenant, TenantId.DefaultId, StringComparison.Ordinal));
    }

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        if (!_catalog.IsTenancyActive || _catalog.SelfService is null)
        {
            return AreaAvailability.Hidden;
        }

        var authenticated = _catalog.Session?.IsAuthenticated == true;
        var user = _catalog.Session?.Username;
        var active = _catalog.ActiveTenant;
        if (_verdict is { } memo
            && memo.Authenticated == authenticated
            && string.Equals(memo.User, user, StringComparison.Ordinal)
            && string.Equals(memo.Active, active, StringComparison.Ordinal))
        {
            return memo.Availability;
        }

        var availability = await ProbeAsync(authenticated, cancellationToken).ConfigureAwait(true);
        _verdict = (authenticated, user, active, availability);
        return availability;
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        if (_catalog.LastStanding is not { } standing)
        {
            return null;
        }

        if (!standing.IsOperator)
        {
            var own = $"You administer tenant {standing.Workspace}.";
            return await _catalog.HasResidencySetAsync(standing.Workspace, cancellationToken).ConfigureAwait(true) == false
                ? own + " It has no residency set."
                : own;
        }

        var tenants = await _catalog.GetTenantsAsync(cancellationToken).ConfigureAwait(true);
        var suspended = tenants.Count(tenant => tenant.Status == TenantLifecycleStatus.Suspended);
        var count = tenants.Count == 1 ? "1 tenant" : $"{TenancyFormat.Count(tenants.Count)} tenants";
        var parts = new List<string>(3) { count };
        if (suspended > 0)
        {
            parts.Add($"{TenancyFormat.Count(suspended)} suspended");
        }

        var survey = await _catalog.GetResidencySurveyAsync(cancellationToken).ConfigureAwait(true);
        if (survey.Unset > 0)
        {
            parts.Add($"{(survey.IsPartial ? "at least " : string.Empty)}{TenancyFormat.Count(survey.Unset)} with no residency set");
        }

        return string.Join(", ", parts) + ".";
    }

    /// <inheritdoc />
    public async ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken)
    {
        if (_catalog.LastStanding is not { IsOperator: true })
        {
            return null;
        }

        var tenants = await _catalog.GetTenantsAsync(cancellationToken).ConfigureAwait(true);
        return TenancyFormat.Count(tenants.Count);
    }

    private async Task<AreaAvailability> ProbeAsync(bool authenticated, CancellationToken cancellationToken)
    {
        try
        {
            var standing = await _catalog.GetStandingAsync(cancellationToken).ConfigureAwait(true);
            if (standing.MaySee)
            {
                return AreaAvailability.Visible;
            }

            return authenticated ? AreaAvailability.Hidden : AreaAvailability.Unavailable(SignInReason);
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
}
