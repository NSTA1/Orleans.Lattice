using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The default <see cref="IExplorerTenantOperatorGate"/> that reads the session's
/// cached <see cref="ExplorerCapabilities.AuthAdminAllowed"/> flag. That flag is
/// set only when the auth-admin control plane accepts the caller as an
/// administrator - the same <c>Admin</c>-on-policy-tree root of trust the
/// cluster's cross-tenant seams validate against - so it is the faithful
/// client-side platform-operator signal.
/// </summary>
/// <remarks>
/// The capability map is a UX affordance whose backing probe is real and
/// server-fail-closed: the cluster re-checks every cross-tenant read regardless of
/// this flag. Basing the operator gate on it therefore adds a fail-closed display
/// layer without weakening - or duplicating - the server's enforcement.
/// </remarks>
internal sealed class CapabilityExplorerTenantOperatorGate(IExplorerCapabilityStore capabilities)
    : IExplorerTenantOperatorGate
{
    private readonly IExplorerCapabilityStore _capabilities =
        capabilities ?? throw new ArgumentNullException(nameof(capabilities));

    /// <inheritdoc />
    public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
        new(_capabilities.Current.AuthAdminAllowed);
}
