using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// One enabled tenant install whose tool activation succeeded: the shared activation and
/// the manifest's roles compiled for the install's tenant, indexed as the manifest
/// declares them.
/// </summary>
internal sealed class AppMcpInstalledApp
{
    /// <summary>Initializes a new <see cref="AppMcpInstalledApp"/>.</summary>
    /// <param name="tenant">The owning tenant.</param>
    /// <param name="activation">The app version's successful activation.</param>
    /// <param name="roles">The manifest roles compiled for <paramref name="tenant"/>, in manifest order.</param>
    public AppMcpInstalledApp(TenantId tenant, AppMcpToolActivation activation, AppMcpRoleGate[] roles)
    {
        ArgumentNullException.ThrowIfNull(activation);
        ArgumentNullException.ThrowIfNull(roles);
        Tenant = tenant;
        Activation = activation;
        Roles = roles;
    }

    /// <summary>The owning tenant.</summary>
    public TenantId Tenant { get; }

    /// <summary>The app version's successful activation.</summary>
    public AppMcpToolActivation Activation { get; }

    /// <summary>The manifest roles compiled for <see cref="Tenant"/>, in manifest order.</summary>
    public AppMcpRoleGate[] Roles { get; }
}
