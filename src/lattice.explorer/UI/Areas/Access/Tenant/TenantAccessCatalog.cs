using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// The circuit's small, memoised view of one tenant's delegated access
/// administration: the posture probe that decides whether the tenant Access pages
/// are shown, and the first page of the tenant's groups and rules that the subject
/// picker and the address line complete against. Every write a tenant page makes
/// invalidates it.
/// </summary>
/// <remarks>
/// <para>
/// Scoped per circuit. Everything remembered is filed under the caller
/// (<see cref="ShellCallerKey"/>) and the tenant it was read for, so an answer
/// read for one sign-in, endpoint or tenant is never served to another.
/// </para>
/// <para>
/// The posture is the one call on the delegated surface that answers while the
/// feature is off, so it tells "off" from "not permitted". Anything it cannot
/// establish - no facade, the reserved default tenant, a fault - reads as
/// <see cref="TenantAccessStanding.Unavailable"/>, which opens nothing.
/// </para>
/// </remarks>
/// <param name="facades">The delegated tenant-access facades.</param>
/// <param name="caller">The circuit's caller, or <see langword="null"/> for one that asserts nothing.</param>
internal sealed class TenantAccessCatalog(ITenantAccessFacades facades, ShellCaller? caller = null)
{
    private static readonly TenantAccessPageRequest CompletionPage = new() { PageSize = TenantAccessPageRequest.MaxPageSize };

    private readonly ITenantAccessFacades _facades = facades ?? throw new ArgumentNullException(nameof(facades));
    private readonly ShellCaller _caller = caller ?? new ShellCaller();
    private ShellCallerKey _memoCaller;
    private TenantAccessState? _state;
    private (string Tenant, IReadOnlyList<TenantGroupDescriptor> Groups)? _groups;
    private (string Tenant, IReadOnlyList<TenantRuleView> Rules)? _rules;

    /// <summary>The tenant directory facade, or <see langword="null"/> when the head serves none.</summary>
    public ILatticeTenantDirectoryAdmin? Directory => _facades.Directory;

    /// <summary>The tenant policy facade, or <see langword="null"/> when the head serves none.</summary>
    public ILatticeTenantPolicyAdmin? Policy => _facades.Policy;

    /// <summary>
    /// Whether the circuit can reach delegated tenant access administration at
    /// all: its head serves the tenant policy facade, whose posture probe decides.
    /// </summary>
    public bool IsServed => _facades.Policy is not null;

    /// <summary>
    /// Reads the caller's standing towards <paramref name="tenant"/> from the
    /// posture probe, memoised for the caller and tenant.
    /// </summary>
    /// <param name="tenant">The tenant a tenant-rooted address names.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The state; never <see langword="null"/>.</returns>
    /// <exception cref="OperationCanceledException">The read was cancelled.</exception>
    public async ValueTask<TenantAccessState> GetStateAsync(string tenant, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        var key = ForgetIfTheCallerChanged();
        if (_state is { } remembered && string.Equals(remembered.Tenant, tenant, StringComparison.Ordinal))
        {
            return remembered;
        }

        if (_facades.Policy is not { } policy || !Administers(tenant))
        {
            // Nothing to ask, so nothing that could change: remember it.
            return Remember(key, TenantAccessState.Unavailable(tenant));
        }

        TenantAccessState state;
        try
        {
            var posture = await policy.GetPostureAsync(tenant, cancellationToken).ConfigureAwait(true);
            if (posture is null)
            {
                // An absent answer proved nothing, so it admits nothing - and is asked again.
                return TenantAccessState.Unavailable(tenant);
            }

            state = TenantAccessState.From(tenant, posture);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            state = new TenantAccessState(tenant, TenantAccessStanding.NotPermitted);
        }
        catch (TenantAccessAdministrationDisabledException)
        {
            state = new TenantAccessState(tenant, TenantAccessStanding.Off);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // A fault proves nothing: shown as unavailable, and asked again next time.
            return TenantAccessState.Unavailable(tenant);
        }

        return Remember(key, state);
    }

    /// <summary>
    /// The tenant <paramref name="scope"/> names when it is delegated to the
    /// caller, or <see langword="null"/>: a cluster-wide page (no scope), or a
    /// tenant the caller may not administer here, keeps its cluster-wide page.
    /// </summary>
    /// <param name="scope">The tenant a page's address is rooted at, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The state for <paramref name="scope"/>, or <see langword="null"/> on a cluster-wide page.</returns>
    public async ValueTask<TenantAccessState?> GetStateForScopeAsync(string? scope, CancellationToken cancellationToken) =>
        scope is null ? null : await GetStateAsync(scope, cancellationToken).ConfigureAwait(true);

    /// <summary>
    /// The first page of <paramref name="tenant"/>'s own groups, for the subject
    /// picker and completion. The facade composes every id under the tenant named,
    /// so no other tenant's group can appear.
    /// </summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The groups, in ascending order of local name.</returns>
    /// <exception cref="NotSupportedException">The head serves no tenant directory.</exception>
    public async Task<IReadOnlyList<TenantGroupDescriptor>> GetGroupsAsync(string tenant, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        var key = ForgetIfTheCallerChanged();
        if (_groups is { } remembered && string.Equals(remembered.Tenant, tenant, StringComparison.Ordinal))
        {
            return remembered.Groups;
        }

        var directory = _facades.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
        var page = await directory.ListGroupsAsync(tenant, CompletionPage, cancellationToken).ConfigureAwait(true);
        IReadOnlyList<TenantGroupDescriptor> groups = page?.Entries ?? [];
        if (ForgetIfTheCallerChanged() == key)
        {
            _groups = (tenant, groups);
        }

        return groups;
    }

    /// <summary>The first page of the rules governing <paramref name="tenant"/>, for completion.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The rules: the tenant's own tier and the platform rules scoped to its trees.</returns>
    /// <exception cref="NotSupportedException">The head serves no tenant policy.</exception>
    public async Task<IReadOnlyList<TenantRuleView>> GetRulesAsync(string tenant, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        var key = ForgetIfTheCallerChanged();
        if (_rules is { } remembered && string.Equals(remembered.Tenant, tenant, StringComparison.Ordinal))
        {
            return remembered.Rules;
        }

        var policy = _facades.Policy ?? throw new NotSupportedException("This Explorer does not serve tenant rules.");
        var page = await policy.ListRulesAsync(tenant, CompletionPage, cancellationToken).ConfigureAwait(true);
        IReadOnlyList<TenantRuleView> rules = page?.Entries ?? [];
        if (ForgetIfTheCallerChanged() == key)
        {
            _rules = (tenant, rules);
        }

        return rules;
    }

    /// <summary>Forgets everything remembered after a write, including the posture whose usage figures it changed.</summary>
    public void Invalidate()
    {
        _state = null;
        _groups = null;
        _rules = null;
    }

    /// <summary>
    /// Whether <paramref name="tenant"/> can have delegated access administration:
    /// a valid tenant id other than the reserved default, which has none.
    /// </summary>
    /// <param name="tenant">The tenant.</param>
    public static bool Administers(string? tenant) =>
        tenant is not null
        && TenantId.TryParse(tenant, out _)
        && !string.Equals(tenant, TenantId.DefaultId, StringComparison.Ordinal);

    private TenantAccessState Remember(ShellCallerKey key, TenantAccessState state)
    {
        if (ForgetIfTheCallerChanged() == key)
        {
            _state = state;
        }

        return state;
    }

    /// <summary>Forgets everything read for another caller, and returns the caller now.</summary>
    private ShellCallerKey ForgetIfTheCallerChanged()
    {
        var key = _caller.Current;
        if (_memoCaller != key)
        {
            _memoCaller = key;
            _state = null;
            _groups = null;
            _rules = null;
        }

        return key;
    }
}
