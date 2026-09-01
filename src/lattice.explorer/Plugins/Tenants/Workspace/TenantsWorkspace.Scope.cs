using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;

/// <summary>
/// The tenant scope: making one of the listed tenants the tenant the whole
/// Explorer reads as.
/// </summary>
/// <remarks>
/// <para>
/// <b>One source of truth, and only one.</b> This does not maintain a second
/// notion of "the active tenant" beside the shell's picker. It calls
/// <c>SwitchTenantAsync</c> on the plugin's own controlled domain contract, which
/// delegates to the Explorer's <c>IExplorerTenantSwitcher</c> - the same seam the
/// picker mutates and the same seam the identity resolver validates a remembered
/// tenant against. The list and the picker therefore cannot disagree: they are
/// two views of one value, which is exactly what was missing when this area could
/// list every tenant and still not switch to one.
/// </para>
/// <para>
/// <b>The refusal is real and is stated.</b> The switch is operator-gated and
/// fails closed, so a caller this cluster does not validate as a platform
/// operator changes nothing. The switcher publishes that refusal as a scope
/// notice for the shell to announce; this records it as the area's own status
/// too, so the answer is beside the button that was pressed and not only in the
/// chrome.
/// </para>
/// <para>
/// <b>It is advisory, not enforcement</b> (epic decision D6). The cluster
/// re-enforces every read and every mutation against the scope it resolves
/// itself, so this is a way of asking, never a way of being granted.
/// </para>
/// </remarks>
public sealed partial class TenantsWorkspace
{
    /// <summary>
    /// The status the area reports when the cluster refuses to make a tenant
    /// active. Names the authority rather than the tenant, because the refusal is
    /// about the caller and not about which tenant they picked.
    /// </summary>
    internal const string SwitchRefusedMessage =
        "Switching the active tenant is reserved for platform operators, so the Explorer is "
        + "still reading as the tenant it was.";

    /// <summary>
    /// Whether "set as active tenant" is offered for <paramref name="tenantId"/>:
    /// the gate admits the caller, nothing is in flight, tenant scoping is on,
    /// and the tenant is not already the active one.
    /// </summary>
    /// <remarks>
    /// Advisory only. It decides whether a control renders enabled; the switcher
    /// still validates the caller, so answering <see langword="true"/> here can
    /// never make a refusal succeed.
    /// </remarks>
    /// <param name="tenantId">The tenant the row offers.</param>
    /// <returns><see langword="true"/> when the action is worth offering.</returns>
    public bool CanSetActiveTenant(string? tenantId) =>
        Allowed
        && !Busy
        && _domain.IsTenancyEnabled
        && tenantId is { Length: > 0 }
        && !IsActiveTenant(tenantId);

    /// <summary>
    /// Whether <paramref name="tenantId"/> is the tenant the Explorer is
    /// currently reading as.
    /// </summary>
    /// <param name="tenantId">The tenant to test.</param>
    /// <returns><see langword="true"/> when it is the active tenant.</returns>
    public bool IsActiveTenant(string? tenantId) =>
        tenantId is { Length: > 0 }
        && string.Equals(ActiveTenantId, tenantId, StringComparison.Ordinal);

    /// <summary>
    /// Makes <paramref name="tenantId"/> the Explorer's active tenant, driving
    /// the shell's tenant picker from the tenant list.
    /// </summary>
    /// <remarks>
    /// A refused switch changes nothing and is reported as a denial: the whole
    /// point of routing through the shared switcher is that the fail-closed
    /// answer is honest rather than a silent no-op.
    /// </remarks>
    /// <param name="tenantId">The tenant to read as.</param>
    /// <exception cref="ArgumentNullException"><paramref name="tenantId"/> is <see langword="null"/>.</exception>
    public async Task SetActiveTenantAsync(string tenantId)
    {
        ArgumentNullException.ThrowIfNull(tenantId);

        ClearResult();

        if (!CanSetActiveTenant(tenantId))
        {
            RaiseChanged();
            return;
        }

        BeginBusy();
        try
        {
            var applied = await _domain
                .SwitchTenantAsync(new ExplorerTenantId(tenantId))
                .ConfigureAwait(false);

            if (applied)
            {
                Report(
                    TenantOperationStatus.Succeeded,
                    ExplorerVocabulary.FormatActiveTenant(tenantId));
            }
            else
            {
                Report(TenantOperationStatus.Denied, SwitchRefusedMessage);
            }
        }
        finally
        {
            EndBusy();
        }
    }
}
