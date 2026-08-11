using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension that adds the <c>Orleans.Lattice.Api.Mcp</c> tree-administration
/// tool module - the <see cref="LatticeApiMcpGroup.TreeAdmin"/> tool group whose
/// tools adapt the tree-administration control facade onto the MCP surface.
/// </summary>
/// <remarks>
/// <para>
/// This foundation module is <b>discoverable but empty</b>: it registers the
/// tree-administration group so an administrator-granted caller sees it advertised
/// in <c>lattice_capabilities</c>, but it contributes no tools yet. The whole-tree
/// lifecycle tools (bulk-load, delete, resize, reshard, and the rest) land in later
/// work, each a thin adapter that defers to the facade's own fail-closed access
/// gate. The module adds no authorization path of its own, and the group is
/// discovered only by a caller granted <see cref="LatticeOperation.Admin"/>.
/// </para>
/// <para>Add it after <c>AddLatticeMcp</c>:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o =&gt; o.RequireAuthorization = true);
/// builder.Services.AddTreeAdminTools();
/// </code>
/// </remarks>
public static class LatticeMcpTreeAdminServiceCollectionExtensions
{
    /// <summary>
    /// Registers the tree-administration tool module as a
    /// <see cref="LatticeApiMcpGroup.TreeAdmin"/> tool group. Registering the group
    /// is what makes it discoverable in <c>lattice_capabilities</c> to an
    /// administrator-granted caller; this foundation module contributes no tools
    /// yet, so the group is advertised with an empty tool set. The whole-tree
    /// lifecycle tools - and any opt-in flag that gates the mutating ones, mirroring
    /// <see cref="LatticeApiMcpOptions.EnableReplicationControlTools"/> - land in
    /// later work.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddTreeAdminTools(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, TreeAdminToolGroup>());

        return services;
    }
}
