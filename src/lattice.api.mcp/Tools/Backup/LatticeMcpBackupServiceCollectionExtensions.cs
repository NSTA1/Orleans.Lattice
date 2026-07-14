using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension that adds the <c>Orleans.Lattice.Api.Mcp</c> backup tool module -
/// the <see cref="LatticeApiMcpGroup.Backup"/> tool group whose tools adapt the
/// backup control facade onto the MCP surface.
/// </summary>
/// <remarks>
/// <para>
/// The module is inspect-only by default: it always contributes the read-only
/// list, describe, inventory, scope-status, and artifact-export tools, and adds
/// the mutating capture, incremental-capture, restore, revert, and delete tools
/// only when backup control is opted in - either by passing
/// <c>enableControl: true</c> here or by setting
/// <see cref="LatticeApiMcpOptions.EnableBackupControlTools"/>. Every mutating
/// tool is annotated destructive and non-read-only.
/// </para>
/// <para>
/// The module adds no authorization path. Each tool stamps the caller credential
/// bridged from the request principal onto the ambient credential context and
/// defers to the facade's own fail-closed backup access gate, so an unauthorized
/// caller is default-denied on every read and mutation.
/// </para>
/// <para>Add it after <c>AddLatticeMcp</c>:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o =&gt; o.RequireAuthorization = true);
/// builder.Services.AddBackupTools(enableControl: true);
/// </code>
/// </remarks>
public static class LatticeMcpBackupServiceCollectionExtensions
{
    /// <summary>
    /// Registers the backup tool module as a <see cref="LatticeApiMcpGroup.Backup"/>
    /// tool group. Sets <see cref="LatticeApiMcpOptions.EnableBackupTools"/>, and
    /// when <paramref name="enableControl"/> is <see langword="true"/> also sets
    /// <see cref="LatticeApiMcpOptions.EnableBackupControlTools"/> so the mutating
    /// control tools are contributed alongside the read-only inspect tools.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="enableControl">
    /// When <see langword="true"/>, the mutating control tools (capture,
    /// incremental capture, restore, revert, delete) are contributed in addition
    /// to the read-only inspect tools. Defaults to <see langword="false"/>
    /// (inspect-only).
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddBackupTools(
        this IServiceCollection services,
        bool enableControl = false)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.Configure<LatticeApiMcpOptions>(options =>
        {
            options.EnableBackupTools = true;
            if (enableControl)
            {
                options.EnableBackupControlTools = true;
            }
        });

        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, BackupToolGroup>());

        return services;
    }
}
