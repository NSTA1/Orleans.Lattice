using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension that adds the <c>Orleans.Lattice.Api.Mcp</c> tree-administration
/// tool module - the <see cref="LatticeApiMcpGroup.TreeAdmin"/> tool group whose
/// first surface adapts the schema-management control facade
/// (<c>ILatticeSchemaControl</c>) onto the MCP surface by delegation.
/// </summary>
/// <remarks>
/// <para>
/// The module is schema-inspect-only by default: it always contributes the
/// read-only schema-inspection tools (policy / version-config / dead-letter /
/// remediation-status / compliance / capability reads), and adds the mutating
/// schema-management tools (set / clear policy, set / clear version config, advance
/// / migrate version, remediate) only when schema control is opted in - either by
/// passing <c>enableSchemaControl: true</c> here or by setting
/// <see cref="LatticeApiMcpOptions.EnableTreeAdminSchemaControlTools"/>. Every
/// mutating tool is annotated destructive and non-read-only.
/// </para>
/// <para>
/// The module adds no authorization path of its own and adds no method to the
/// tree-administration facade: it holds the <c>ILatticeSchemaControl</c> facade,
/// resolved from the request service provider at call time, and each tool defers to
/// the facade's own fail-closed schema access gate, so an unauthorized caller is
/// default-denied on every read and mutation, and the group is discovered only by a
/// caller granted <see cref="LatticeOperation.Admin"/>.
/// </para>
/// <para>Add it after <c>AddLatticeMcp</c> (the host must also register the schema control facade):</para>
/// <code>
/// builder.Services.AddLatticeMcp(o =&gt; o.RequireAuthorization = true);
/// builder.Services.AddTreeAdminTools(enableSchemaControl: true);
/// </code>
/// </remarks>
public static class LatticeMcpTreeAdminServiceCollectionExtensions
{
    /// <summary>
    /// Registers the tree-administration tool module as a
    /// <see cref="LatticeApiMcpGroup.TreeAdmin"/> tool group. Registering the group
    /// is what makes it discoverable in <c>lattice_capabilities</c> to an
    /// administrator-granted caller. The read-only schema-inspection tools are
    /// always contributed; when <paramref name="enableSchemaControl"/> is
    /// <see langword="true"/> this also sets
    /// <see cref="LatticeApiMcpOptions.EnableTreeAdminSchemaControlTools"/> so the
    /// mutating schema-management tools are contributed alongside them. Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="enableSchemaControl">
    /// When <see langword="true"/>, the mutating schema-management tools (set /
    /// clear policy, set / clear version config, advance / migrate version,
    /// remediate) are contributed in addition to the read-only schema-inspection
    /// tools. Defaults to <see langword="false"/> (schema-inspect-only).
    /// </param>
    /// <param name="enableLifecycle">
    /// When <see langword="true"/>, the mutating tree-lifecycle tools (explicit tree
    /// creation, alias assignment, per-tree configuration update) are contributed in
    /// addition to the read-only lifecycle tools (existence, alias resolution, config
    /// read, shard-map read). Defaults to <see langword="false"/>
    /// (lifecycle-read-only).
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddTreeAdminTools(
        this IServiceCollection services,
        bool enableSchemaControl = false,
        bool enableLifecycle = false)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.Configure<LatticeApiMcpOptions>(options =>
        {
            if (enableSchemaControl)
            {
                options.EnableTreeAdminSchemaControlTools = true;
            }

            if (enableLifecycle)
            {
                options.EnableTreeAdminLifecycleTools = true;
            }
        });

        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, TreeAdminToolGroup>());

        return services;
    }
}
