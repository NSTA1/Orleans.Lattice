using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// The Tenancy area's per-circuit view of the cluster: the tenant facades (any
/// of which a head may not register), the caller's <see cref="TenancyStanding"/>,
/// and the memoised list of tenants the caller can reach, which the directory,
/// the address completions and the tenant scope selector all read, so they can
/// never disagree.
/// </summary>
/// <remarks>
/// The chrome's tenancy reading is resolved lazily, at call time, because it in
/// turn reads the accessible-tenant list this catalogue supplies.
/// </remarks>
internal sealed class TenancyCatalog
{
    private readonly IServiceProvider _services;
    private (bool Authenticated, string? User, string? Active, TenancyStanding Standing)? _standing;
    private (bool Authenticated, string? User, string? Active, IReadOnlyList<TenantDescriptor> Tenants)? _tenants;
    private (string Tenant, int? Count)? _apps;

    /// <summary>Creates the catalogue over the circuit's services.</summary>
    /// <param name="services">The circuit's services.</param>
    public TenancyCatalog(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
        SelfService = services.GetShellFacade<ILatticeTenantSelfService>();
        Admin = services.GetShellFacade<ILatticeTenantAdmin>();
        Access = services.GetShellFacade<ILatticeTenantAccessAdmin>();
        Grants = services.GetShellFacade<ILatticeTenantGrantAdmin>();
        Regions = services.GetShellFacade<ILatticeTenantRegionAdmin>();
        Quota = services.GetShellFacade<ILatticeTenantQuotaUsage>();
        Apps = services.GetShellFacade<ILatticeAppsControl>();
        Session = services.GetService<IExplorerAuthSession>();
    }

    /// <summary>The read-only self-service facade, or <see langword="null"/> when not registered.</summary>
    public ILatticeTenantSelfService? SelfService { get; }

    /// <summary>The tenant lifecycle facade, or <see langword="null"/>.</summary>
    public ILatticeTenantAdmin? Admin { get; }

    /// <summary>The admin-subject facade, or <see langword="null"/>.</summary>
    public ILatticeTenantAccessAdmin? Access { get; }

    /// <summary>The cross-tenant grant facade, or <see langword="null"/>.</summary>
    public ILatticeTenantGrantAdmin? Grants { get; }

    /// <summary>The region-residency facade, or <see langword="null"/>.</summary>
    public ILatticeTenantRegionAdmin? Regions { get; }

    /// <summary>The quota-usage facade, or <see langword="null"/>.</summary>
    public ILatticeTenantQuotaUsage? Quota { get; }

    /// <summary>The apps control facade, read only for installed-app counts, or <see langword="null"/>.</summary>
    public ILatticeAppsControl? Apps { get; }

    /// <summary>The Explorer's sign-in, or <see langword="null"/>.</summary>
    public IExplorerAuthSession? Session { get; }

    /// <summary>Whether tenancy is on for this head and circuit.</summary>
    public bool IsTenancyActive => _services.GetService<ExplorerTenancy>()?.IsActive == true;

    /// <summary>The tenant the Explorer is scoped to, or <see langword="null"/>.</summary>
    public string? ActiveTenant => _services.GetService<ExplorerTenancy>()?.ActiveTenant;

    /// <summary>The last standing proven on this circuit, or <see langword="null"/> before the first.</summary>
    public TenancyStanding? LastStanding => _standing?.Standing;

    /// <summary>
    /// The caller's standing, proven with the cheapest reads that establish it
    /// and remembered until the identity or the active tenant changes. A fault
    /// propagates, and nothing is remembered from it.
    /// </summary>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <exception cref="NotSupportedException">The head registered no self-service facade.</exception>
    public async Task<TenancyStanding> GetStandingAsync(CancellationToken cancellationToken)
    {
        var selfService = SelfService ?? throw new NotSupportedException(TenancyFailure.NotServedMessage);
        var authenticated = Session?.IsAuthenticated == true;
        var user = Session?.Username;
        var active = ActiveTenant;
        if (_standing is { } memo
            && memo.Authenticated == authenticated
            && string.Equals(memo.User, user, StringComparison.Ordinal)
            && string.Equals(memo.Active, active, StringComparison.Ordinal))
        {
            return memo.Standing;
        }

        var current = await selfService.GetCurrentTenantAsync(cancellationToken).ConfigureAwait(true)
            ?? throw new InvalidOperationException("The cluster did not report the caller's tenant.");
        var isOperator = await IsOperatorAsync(cancellationToken).ConfigureAwait(true);
        var workspace = active ?? current.TenantId;
        var isAdmin = !isOperator && await AdministersAsync(workspace, cancellationToken).ConfigureAwait(true);

        var standing = new TenancyStanding(isOperator, current.TenantId, active, isAdmin);
        _standing = (authenticated, user, active, standing);
        return standing;
    }

    /// <summary>
    /// The tenants the caller can reach, ascending by id, remembered until
    /// <see cref="Invalidate"/> or until the identity or the active tenant changes,
    /// so a sign-in, a sign-out or a new identity never reads the previous
    /// caller's list. A fault propagates, and nothing is remembered.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<IReadOnlyList<TenantDescriptor>> GetTenantsAsync(CancellationToken cancellationToken)
    {
        var authenticated = Session?.IsAuthenticated == true;
        var user = Session?.Username;
        var active = ActiveTenant;
        if (_tenants is { } memo
            && memo.Authenticated == authenticated
            && string.Equals(memo.User, user, StringComparison.Ordinal)
            && string.Equals(memo.Active, active, StringComparison.Ordinal))
        {
            return memo.Tenants;
        }

        var selfService = SelfService ?? throw new NotSupportedException(TenancyFailure.NotServedMessage);
        var tenants = await selfService.ListAccessibleTenantsAsync(cancellationToken).ConfigureAwait(true) ?? [];
        IReadOnlyList<TenantDescriptor> sorted = [.. tenants.Where(tenant => tenant is not null).OrderBy(tenant => tenant.TenantId, StringComparer.Ordinal)];
        _tenants = (authenticated, user, active, sorted);
        return sorted;
    }

    /// <summary>
    /// How many apps are installed for <paramref name="tenantId"/>, or
    /// <see langword="null"/> when that cannot be read: the apps facade lists the
    /// caller's own tenant only, so any other tenant's count is unknown here, and
    /// a caller without the install grant learns nothing.
    /// </summary>
    /// <param name="tenantId">The tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<int?> GetInstalledAppCountAsync(string tenantId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (Apps is null || LastStanding is not { } standing || !string.Equals(standing.CurrentTenant, tenantId, StringComparison.Ordinal))
        {
            return null;
        }

        if (_apps is { } memo && string.Equals(memo.Tenant, tenantId, StringComparison.Ordinal))
        {
            return memo.Count;
        }

        int? count;
        try
        {
            var catalog = await Apps.ListAsync(cancellationToken).ConfigureAwait(true);
            count = catalog?.Apps.Length;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            count = null;
        }

        _apps = (tenantId, count);
        return count;
    }

    /// <summary>Forgets the tenant list and app counts, so the next read reflects a change just made.</summary>
    public void Invalidate()
    {
        _tenants = null;
        _apps = null;
    }

    /// <summary>Forgets the proven standing, so the next read proves it again.</summary>
    public void InvalidateStanding() => _standing = null;

    /// <summary>
    /// Whether the caller has platform-operator standing, through the
    /// operator-gated tenant switcher. Every fault, and a head without tenancy,
    /// reads as "not an operator".
    /// </summary>
    /// <param name="cancellationToken">Cancels the check.</param>
    /// <returns><see langword="true"/> only for a proven operator.</returns>
    internal async Task<bool> IsOperatorAsync(CancellationToken cancellationToken)
    {
        if (_services.GetService<IExplorerTenantSwitcher>() is not { } switcher)
        {
            return false;
        }

        try
        {
            return await switcher.IsOperatorAsync(cancellationToken).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return false;
        }
    }

    private async Task<bool> AdministersAsync(string tenantId, CancellationToken cancellationToken)
    {
        if (Access is null || string.Equals(tenantId, TenantId.DefaultId, StringComparison.Ordinal))
        {
            return false;
        }

        try
        {
            var report = await Access.ListAdminSubjectsAsync(tenantId, cancellationToken).ConfigureAwait(true);
            return report is not null;
        }
        catch (Exception exception) when (exception is LatticeAuthorizationDeniedException or TenantNotFoundException)
        {
            return false;
        }
    }
}
