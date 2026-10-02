using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// A tenant's member set (<c>/t/{tenant}/access/members</c>), the one Access
/// address with no cluster-wide form: the tenant Members view when the tenant's
/// access administration is delegated to the caller, and otherwise one sentence
/// saying why it is not administered here.
/// </summary>
public partial class AccessMembersPage
{
    private readonly AccessTenantGate _gate = new();

    [Inject]
    internal TenantAccessCatalog TenantAccess { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    private string? Scope => Address.Tenant;

    private TenantAccessState? State => _gate.State;

    private string? Delegated => _gate.DelegatedTenant;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Scope is not { } scope)
        {
            // A member set belongs to a tenant, so only a tenant-rooted address names one.
            Navigation.NotFound();
            return;
        }

        await _gate.ResolveAsync(TenantAccess, scope).ConfigureAwait(true);
    }
}
