using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// Completes the address line against the tenants the caller can reach:
/// <c>t/{tenant}</c> opens that tenant's workspace (switching to it through the
/// operator-gated switch when it is not the active one), and, for a platform
/// operator, <c>tenancy/{tenant}</c> opens its administration. Typing either
/// prefix narrows to that kind; plain text matches both, prefix matches first. A
/// raw <c>/tenancy/</c> address completes the administration pages.
/// </summary>
/// <remarks>
/// It offers exactly the tenants the catalogue lists, which is the same list the
/// tenant scope selector offers, so a completion never names a tenant the
/// selector would not.
/// </remarks>
/// <param name="catalog">The circuit's tenancy catalogue.</param>
internal sealed class TenancyCompletionSource(TenancyCatalog catalog) : IAddressCompletionSource
{
    /// <summary>The label prefix of a my-tenant completion.</summary>
    public const string WorkspacePrefix = "t/";

    /// <summary>The label prefix of an administration completion.</summary>
    public const string AdministrationPrefix = TenancyRoutes.AreaKey + "/";

    private static readonly string AdministrationAddressPrefix = TenancyRoutes.Directory.Format() + "/";

    private readonly TenancyCatalog _catalog = catalog ?? throw new ArgumentNullException(nameof(catalog));

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<AddressCompletion>> CompleteAsync(AddressQuery query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (!TryReadTerm(query, out var term, out var wantWorkspace, out var wantAdministration))
        {
            return [];
        }

        var standing = await _catalog.GetStandingAsync(cancellationToken).ConfigureAwait(true);
        wantAdministration &= standing.IsOperator;
        if (!wantWorkspace && !wantAdministration)
        {
            return [];
        }

        var tenants = await _catalog.GetTenantsAsync(cancellationToken).ConfigureAwait(true);
        var first = new List<AddressCompletion>(query.Limit);
        var later = new List<AddressCompletion>();
        foreach (var tenant in tenants)
        {
            var rank = Rank(term, tenant.TenantId);
            if (rank < 0)
            {
                continue;
            }

            var bucket = rank == 0 ? first : later;
            if (wantWorkspace)
            {
                bucket.Add(new AddressCompletion(WorkspacePrefix + tenant.TenantId, TenancyRoutes.MyTenant(tenant.TenantId), WorkspaceDetail(tenant, standing)));
            }

            if (wantAdministration)
            {
                bucket.Add(new AddressCompletion(AdministrationPrefix + tenant.TenantId, TenancyRoutes.Tenant(tenant.TenantId), $"Administer tenant {tenant.TenantId}"));
            }
        }

        first.AddRange(later);
        return first.Count > query.Limit ? first.GetRange(0, query.Limit) : first;
    }

    private static string WorkspaceDetail(TenantDescriptor tenant, TenancyStanding standing)
    {
        var state = TenancyFormat.TenantStateLabel(tenant.Status);
        return string.Equals(tenant.TenantId, standing.Workspace, StringComparison.Ordinal)
            ? $"Your tenant, {state.ToLowerInvariant()}"
            : $"Switch to this tenant, {state.ToLowerInvariant()}";
    }

    private static bool TryReadTerm(AddressQuery query, out string term, out bool wantWorkspace, out bool wantAdministration)
    {
        var text = query.Text.Trim();
        term = text;
        wantWorkspace = true;
        wantAdministration = true;

        switch (query.Mode)
        {
            case AddressQueryMode.Search:
                if (text.StartsWith(WorkspacePrefix, StringComparison.OrdinalIgnoreCase))
                {
                    term = text[WorkspacePrefix.Length..];
                    wantAdministration = false;
                    return true;
                }

                if (text.StartsWith(AdministrationPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    term = text[AdministrationPrefix.Length..];
                    wantWorkspace = false;
                    return true;
                }

                return text.Length > 0;

            case AddressQueryMode.Address when text.StartsWith(AdministrationAddressPrefix, StringComparison.OrdinalIgnoreCase):
                term = Uri.UnescapeDataString(text[AdministrationAddressPrefix.Length..]);
                wantWorkspace = false;
                return true;

            default:
                return false;
        }
    }

    // 0: the id starts with the term; 1: it contains it; -1: no match.
    private static int Rank(string term, string tenantId) =>
        term.Length == 0 || tenantId.StartsWith(term, StringComparison.OrdinalIgnoreCase) ? 0
        : tenantId.Contains(term, StringComparison.OrdinalIgnoreCase) ? 1
        : -1;
}
