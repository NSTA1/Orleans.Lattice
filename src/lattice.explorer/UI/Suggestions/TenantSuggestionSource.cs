using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Suggestions;

/// <summary>
/// The tenants the caller may reach: the same list the address line's tenant
/// selector reads, so a picker never offers a tenant the caller cannot reach.
/// </summary>
/// <remarks>Without tenancy there is no tenant to choose, and the field says so.</remarks>
/// <param name="tenancy">The caller's tenancy.</param>
/// <param name="tenant">The circuit's asserted tenant.</param>
/// <param name="time">The clock the freshness window is measured on.</param>
internal sealed class TenantSuggestionSource(ExplorerTenancy tenancy, ShellAssertedTenant? tenant, TimeProvider? time)
    : CachedSuggestionSource(tenant, time)
{
    /// <summary>The detail beside the active tenant.</summary>
    public const string ActiveDetail = "Active tenant";

    /// <inheritdoc />
    protected override string UnavailableReason => tenancy.IsActive
        ? "The tenants could not be listed, so the tenant id is used as typed."
        : "Tenancy is off, so there is no tenant to choose; the id is used as typed.";

    /// <inheritdoc />
    protected override async Task<IReadOnlyList<LtSuggestion>?> LoadAsync(CancellationToken cancellationToken)
    {
        if (!tenancy.IsActive)
        {
            return null;
        }

        var tenants = await tenancy.GetAccessibleTenantsAsync(cancellationToken).ConfigureAwait(false);
        var active = tenancy.ActiveTenant;
        var values = new List<LtSuggestion>(tenants.Count);
        foreach (var id in tenants)
        {
            var current = string.Equals(id, active, StringComparison.Ordinal);
            values.Add(new LtSuggestion(id, current ? ActiveDetail : null) { Current = current });
        }

        return values;
    }
}
