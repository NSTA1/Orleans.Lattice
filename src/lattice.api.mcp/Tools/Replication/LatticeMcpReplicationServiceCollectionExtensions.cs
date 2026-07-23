using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension that adds the <c>Orleans.Lattice.Api.Mcp</c> replication tool
/// module - the <see cref="LatticeApiMcpGroup.Replication"/> tool group whose
/// tools adapt the replication control facade onto the MCP surface.
/// </summary>
/// <remarks>
/// <para>
/// The module is inspect-only by default: it always contributes the read-only
/// <c>lattice_replication_get_config</c> tool, and adds the mutating
/// <c>lattice_replication_enable</c> and <c>lattice_replication_disable</c> tools
/// only when replication control is opted in - either by passing
/// <c>enableControl: true</c> here or by setting
/// <see cref="LatticeApiMcpOptions.EnableReplicationControlTools"/>. Every
/// mutating tool is annotated destructive and non-read-only.
/// </para>
/// <para>
/// The module adds no authorization path. Each tool stamps the caller credential
/// bridged from the request principal onto the ambient credential context and
/// defers to the facade's own fail-closed replication access gate, so an
/// unauthorized caller is default-denied on every read and mutation, and the
/// group is discovered only by a caller granted
/// <see cref="LatticeOperation.Replication"/>.
/// </para>
/// <para>Add it after <c>AddLatticeMcp</c>:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o =&gt; o.RequireAuthorization = true);
/// builder.Services.AddReplicationTools(enableControl: true);
/// </code>
/// </remarks>
public static class LatticeMcpReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Registers the replication tool module as a
    /// <see cref="LatticeApiMcpGroup.Replication"/> tool group. Sets
    /// <see cref="LatticeApiMcpOptions.EnableReplicationTools"/>, and when
    /// <paramref name="enableControl"/> is <see langword="true"/> also sets
    /// <see cref="LatticeApiMcpOptions.EnableReplicationControlTools"/> so the
    /// mutating control tools are contributed alongside the read-only inspect
    /// tool.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="enableControl">
    /// When <see langword="true"/>, the mutating control tools (enable, disable)
    /// are contributed in addition to the read-only inspect tool. Defaults to
    /// <see langword="false"/> (inspect-only).
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddReplicationTools(
        this IServiceCollection services,
        bool enableControl = false)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.Configure<LatticeApiMcpOptions>(options =>
        {
            options.EnableReplicationTools = true;
            if (enableControl)
            {
                options.EnableReplicationControlTools = true;
            }
        });

        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, ReplicationToolGroup>());

        return services;
    }
}
