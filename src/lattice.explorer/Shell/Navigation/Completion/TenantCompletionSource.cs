using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Navigation.Completion;

/// <summary>
/// The chrome's own completion source for <c>t/</c>: the tenants the caller may
/// reach, each re-rooting the current address at that tenant.
/// </summary>
/// <remarks>
/// It never offers a tenant the accessible-tenant source did not report, and
/// choosing one still goes through the operator-gated switch when the address is
/// resolved, so a completion can never scope a caller beyond what they may reach.
/// </remarks>
internal sealed class TenantCompletionSource : IAddressCompletionSource
{
    private readonly ExplorerTenancy _tenancy;
    private readonly ExplorerNavigator _navigator;

    /// <summary>Creates the source.</summary>
    /// <param name="tenancy">The caller's tenancy.</param>
    /// <param name="navigator">Re-roots the current address at a tenant.</param>
    public TenantCompletionSource(ExplorerTenancy tenancy, ExplorerNavigator navigator)
    {
        ArgumentNullException.ThrowIfNull(tenancy);
        ArgumentNullException.ThrowIfNull(navigator);

        _tenancy = tenancy;
        _navigator = navigator;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);

        if (!_tenancy.IsActive)
        {
            return [];
        }

        var tenants = await _tenancy.GetAccessibleTenantsAsync(cancellationToken).ConfigureAwait(false);
        var active = _tenancy.ActiveTenant;

        return
        [
            .. tenants
                .Where(tenant => tenant.Contains(query.Text, StringComparison.OrdinalIgnoreCase))
                .Take(query.Limit)
                .Select(tenant => new AddressCompletion(
                    ExplorerAddress.TenantSegment + "/" + tenant,
                    _navigator.ReRoot(query.Current, tenant),
                    string.Equals(tenant, active, StringComparison.Ordinal) ? "Active tenant" : "Switch to this tenant")),
        ];
    }
}
