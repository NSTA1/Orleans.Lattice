using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension that adds the <c>Orleans.Lattice.Api.Mcp</c> tenant-admin tool
/// module - the <see cref="LatticeApiMcpGroup.TenantAdmin"/> tool group whose
/// tools adapt the tenant-admin control facade onto the MCP surface.
/// </summary>
/// <remarks>
/// <para>
/// The tenant lifecycle is all-mutating (create, suspend, resume, delete), so the
/// module contributes tools only when tenant-admin control is opted in - either by
/// passing <c>enableControl: true</c> here or by setting
/// <see cref="LatticeApiMcpOptions.EnableTenantAdminControlTools"/>. Registering
/// the group advertises the <c>tenantadmin</c> capability to a caller granted
/// <see cref="LatticeOperation.Admin"/>; a cluster that never calls this method
/// exposes no tenant-admin capability and no tenant-admin tools at all
/// (fail-closed, byte-for-byte unchanged versus before). Every tool is annotated
/// destructive and non-read-only.
/// </para>
/// <para>
/// The module adds no authorization path. Each tool stamps the caller credential
/// bridged from the request principal onto the ambient credential context and
/// defers to the facade's own fail-closed tenant-admin access gate, so an
/// unauthorized caller is default-denied on every mutation, and the group is
/// discovered only by a caller granted <see cref="LatticeOperation.Admin"/>.
/// </para>
/// <para>Add it after <c>AddLatticeMcp</c>:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o =&gt; o.RequireAuthorization = true);
/// builder.Services.AddTenantAdminTools(enableControl: true);
/// </code>
/// </remarks>
public static class LatticeMcpTenantAdminServiceCollectionExtensions
{
    /// <summary>
    /// Registers the tenant-admin tool module as a
    /// <see cref="LatticeApiMcpGroup.TenantAdmin"/> tool group. Sets
    /// <see cref="LatticeApiMcpOptions.EnableTenantAdminTools"/> so the capability
    /// is advertised, and when <paramref name="enableControl"/> is
    /// <see langword="true"/> also sets
    /// <see cref="LatticeApiMcpOptions.EnableTenantAdminControlTools"/> so the
    /// mutating lifecycle tools are contributed.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="enableControl">
    /// When <see langword="true"/>, the mutating lifecycle tools (create, suspend,
    /// resume, delete) are contributed. Defaults to <see langword="false"/>, in
    /// which case the capability is advertised but the group contributes no tools.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddTenantAdminTools(
        this IServiceCollection services,
        bool enableControl = false)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.Configure<LatticeApiMcpOptions>(options =>
        {
            options.EnableTenantAdminTools = true;
            if (enableControl)
            {
                options.EnableTenantAdminControlTools = true;
            }
        });

        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, TenantAdminToolGroup>());

        return services;
    }
}
