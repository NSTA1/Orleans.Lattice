using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The posture gate a tenant-rooted Access page consults before it renders: it
/// reads the caller's standing towards the page's tenant once per scope, and
/// names the tenant when its access administration is delegated to the caller,
/// so the page shows that tenant's view. Every other answer - a cluster-wide
/// page, the feature off, a caller who may not administer the tenant, no
/// answer - keeps the page's cluster-wide behaviour.
/// </summary>
/// <remarks>
/// One per page instance. A page is reused when only its tenant root changes, so
/// the gate re-reads whenever the scope it was asked for differs.
/// </remarks>
internal sealed class AccessTenantGate
{
    private string? _scope;
    private bool _resolved;

    /// <summary>The standing read for the current scope, or <see langword="null"/> on a cluster-wide page or before it is read.</summary>
    public TenantAccessState? State { get; private set; }

    /// <summary>Whether the standing is being read now; the page shows its loading state meanwhile.</summary>
    public bool Resolving { get; private set; }

    /// <summary>The tenant whose view the page shows, or <see langword="null"/> when the page keeps its cluster-wide behaviour.</summary>
    public string? DelegatedTenant => State is { IsDelegated: true } state ? state.Tenant : null;

    /// <summary>
    /// Reads the caller's standing towards <paramref name="scope"/>, unless it was
    /// already read for that scope.
    /// </summary>
    /// <param name="catalog">The circuit's tenant access catalogue.</param>
    /// <param name="scope">The tenant the page's address is rooted at, or <see langword="null"/> on a cluster-wide page.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns><see langword="true"/> when the page shows the tenant's delegated view.</returns>
    public async Task<bool> ResolveAsync(TenantAccessCatalog catalog, string? scope, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        if (_resolved && string.Equals(_scope, scope, StringComparison.Ordinal))
        {
            return DelegatedTenant is not null;
        }

        _resolved = true;
        _scope = scope;
        State = null;
        if (scope is null)
        {
            return false;
        }

        Resolving = true;
        try
        {
            var state = await catalog.GetStateAsync(scope, cancellationToken).ConfigureAwait(true);
            if (string.Equals(_scope, scope, StringComparison.Ordinal))
            {
                State = state;
            }
        }
        finally
        {
            Resolving = false;
        }

        return DelegatedTenant is not null;
    }

    /// <summary>The <c>data-lt-tenant-access</c> value naming a standing.</summary>
    /// <param name="standing">The standing.</param>
    /// <returns><c>off</c>, <c>not-permitted</c>, <c>delegated</c> or <c>unavailable</c>.</returns>
    public static string StandingAttribute(TenantAccessStanding standing) => standing switch
    {
        TenantAccessStanding.Off => "off",
        TenantAccessStanding.NotPermitted => "not-permitted",
        TenantAccessStanding.Delegated => "delegated",
        _ => "unavailable",
    };

    /// <summary>The sentence a tenant page says when the tenant's <paramref name="what"/> is not administered here.</summary>
    /// <param name="state">The standing read.</param>
    /// <param name="what">What would have been administered, such as "member set".</param>
    /// <returns>The sentence.</returns>
    public static string Unavailable(TenantAccessState state, string what)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentException.ThrowIfNullOrEmpty(what);
        return state.Standing switch
        {
            TenantAccessStanding.NotPermitted => $"Tenant {state.Tenant}'s {what} is administered by its administrators, and you are not one of them.",
            TenantAccessStanding.Off => $"Delegated tenant access administration is off, so tenant {state.Tenant}'s {what} is not administered here.",
            _ => $"Tenant {state.Tenant}'s {what} is not administered here.",
        };
    }
}
