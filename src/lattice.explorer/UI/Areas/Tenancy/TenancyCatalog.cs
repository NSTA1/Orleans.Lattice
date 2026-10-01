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
/// turn reads the accessible-tenant list this catalogue supplies. Every memo is
/// filed under the caller (<see cref="Caller"/>: the sign-in, the endpoint and the
/// asserted tenant) and the active tenant, so no answer outlives the caller it was
/// read for.
/// </remarks>
internal sealed class TenancyCatalog
{
    /// <summary>The most tenants whose residency the Home status reads.</summary>
    public const int ResidencySurveyLimit = 50;

    private readonly IServiceProvider _services;
    private (MemoKey Key, TenancyStanding Standing)? _standing;
    private (MemoKey Key, IReadOnlyList<TenantDescriptor> Tenants)? _tenants;
    private (MemoKey Key, string Tenant, int? Count)? _apps;
    private (MemoKey Key, TenancyResidencySurvey Survey)? _survey;

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
        Caller = ShellCaller.Of(services);
    }

    /// <summary>The circuit's caller, which every memo here is filed under.</summary>
    public ShellCaller Caller { get; }

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

    /// <summary>
    /// The last standing proven for the caller and active tenant now, or
    /// <see langword="null"/> before the first; a standing proven for another caller
    /// is never returned.
    /// </summary>
    public TenancyStanding? LastStanding => _standing is { } memo && memo.Key == Key() ? memo.Standing : null;

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
        var key = Key();
        var active = key.Active;
        if (_standing is { } memo && memo.Key == key)
        {
            return memo.Standing;
        }

        var current = await selfService.GetCurrentTenantAsync(cancellationToken).ConfigureAwait(true)
            ?? throw new InvalidOperationException("The cluster did not report the caller's tenant.");
        var isOperator = await IsOperatorAsync(cancellationToken).ConfigureAwait(true);
        var workspace = active ?? current.TenantId;
        var isAdmin = !isOperator && await AdministersAsync(workspace, cancellationToken).ConfigureAwait(true);

        var standing = new TenancyStanding(isOperator, current.TenantId, active, isAdmin);
        if (Key() == key)
        {
            _standing = (key, standing);
        }

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
        var key = Key();
        if (_tenants is { } memo && memo.Key == key)
        {
            return memo.Tenants;
        }

        var selfService = SelfService ?? throw new NotSupportedException(TenancyFailure.NotServedMessage);
        var tenants = await selfService.ListAccessibleTenantsAsync(cancellationToken).ConfigureAwait(true) ?? [];
        IReadOnlyList<TenantDescriptor> sorted = [.. tenants.Where(tenant => tenant is not null).OrderBy(tenant => tenant.TenantId, StringComparer.Ordinal)];
        if (Key() == key)
        {
            _tenants = (key, sorted);
        }

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

        var key = Key();
        if (_apps is { } memo && memo.Key == key && string.Equals(memo.Tenant, tenantId, StringComparison.Ordinal))
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

        if (Key() == key)
        {
            _apps = (key, tenantId, count);
        }

        return count;
    }

    /// <summary>Forgets the tenant list and app counts, so the next read reflects a change just made.</summary>
    public void Invalidate()
    {
        _tenants = null;
        _apps = null;
        _survey = null;
    }

    /// <summary>
    /// How many of the tenants the caller can reach have no residency set, read
    /// from each tenant's status, for at most <see cref="ResidencySurveyLimit"/>
    /// tenants, and remembered for the identity and asserted tenant that read it
    /// until <see cref="Invalidate"/>. A tenant whose status cannot be read is not
    /// counted; the reserved default tenant has no residency and is skipped. A
    /// cancellation propagates, and nothing is remembered.
    /// </summary>
    /// <param name="cancellationToken">Cancels the reads.</param>
    /// <returns>The count, and whether it covers only the first tenants.</returns>
    public async Task<TenancyResidencySurvey> GetResidencySurveyAsync(CancellationToken cancellationToken)
    {
        var key = Key();
        if (_survey is { } memo && memo.Key == key)
        {
            return memo.Survey;
        }

        var tenants = (await GetTenantsAsync(cancellationToken).ConfigureAwait(true)).Where(tenant => !tenant.IsDefault).ToArray();
        var surveyed = tenants.Take(ResidencySurveyLimit).ToArray();
        var answers = await Task.WhenAll(surveyed.Select(tenant => HasResidencySetAsync(tenant.TenantId, cancellationToken))).ConfigureAwait(true);
        var survey = new TenancyResidencySurvey(answers.Count(answer => answer == false), tenants.Length > surveyed.Length);
        if (Key() == key)
        {
            _survey = (key, survey);
        }

        return survey;
    }

    /// <summary>
    /// Whether <paramref name="tenantId"/> has residency set, as the tenancy engine
    /// counts it - any region with a lifecycle status, even one that is Offline or
    /// Removed - or <see langword="null"/> when its status cannot be read.
    /// </summary>
    /// <param name="tenantId">The tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    public async Task<bool?> HasResidencySetAsync(string tenantId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        if (SelfService is null || string.Equals(tenantId, TenantId.DefaultId, StringComparison.Ordinal))
        {
            return null;
        }

        try
        {
            var status = await SelfService.GetTenantAsync(tenantId, cancellationToken).ConfigureAwait(true);
            return status?.Regions is { } regions ? TenancyFormat.HasResidency(regions) : null;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }
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

    private MemoKey Key() => new(Caller.Current, ActiveTenant);

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

    /// <summary>What a memo is filed under: the caller and the active tenant.</summary>
    private readonly record struct MemoKey(ShellCallerKey Caller, string? Active);
}
