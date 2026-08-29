using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The <see cref="IExplorerTenantOperatorGate"/> backed by the Access plugin's
/// own administrator decision: the caller validates as a platform operator
/// exactly when the auth-admin control plane accepted them as an administrator,
/// which is the same <c>Admin</c>-on-policy-tree root of trust the cluster's
/// cross-tenant seams validate against.
/// </summary>
/// <remarks>
/// The decision lives under the Access plugin's own key in the keyed access
/// store, so the gate reads one plugin's published answer rather than a shared
/// capability record every area also writes. It is fail-closed by construction:
/// an unprobed, faulted, or cleared key reads as
/// <see cref="ExplorerPluginAccess.Denied"/>. The store is a UX affordance whose
/// backing probe is real and server-fail-closed - the cluster re-checks every
/// cross-tenant read regardless - so basing the operator gate on it adds a
/// fail-closed display layer without weakening or duplicating the server's
/// enforcement.
/// </remarks>
internal sealed class AccessExplorerTenantOperatorGate(IExplorerPluginAccessStore access)
    : IExplorerTenantOperatorGate
{
    private readonly IExplorerPluginAccessStore _access =
        access ?? throw new ArgumentNullException(nameof(access));

    /// <inheritdoc />
    public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
        new(_access.Get(AccessPluginKeys.PluginId).IsAllowed);
}
