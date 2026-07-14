using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension that opts the read-only state facade into the
/// <c>Orleans.Lattice.Api.Mcp</c> server as a tool module.
/// </summary>
/// <remarks>
/// <para>Call it after <see cref="LatticeMcpServiceCollectionExtensions.AddLatticeMcp"/>:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
/// builder.Services.AddStateTools();
/// </code>
/// <para>
/// The registration is the opt-in: the permission-aware discovery core advertises
/// the state tools only to a caller whose effective permissions grant the state
/// group, so an unauthorised caller can neither see nor invoke them. The tools are
/// read-only adapters over the host's registered <c>ILatticeStateQuery</c> facade;
/// the host must register that facade (for the in-silo topology, via
/// <c>ISiloBuilder.AddLatticeStateApi(...)</c>) for the tools to resolve it at
/// invocation time.
/// </para>
/// </remarks>
public static class LatticeMcpStateToolsServiceCollectionExtensions
{
    /// <summary>
    /// Registers the read-only state tool module (<see cref="LatticeApiMcpGroup.State"/>)
    /// on the MCP server and flips <see cref="LatticeApiMcpOptions.EnableStateTools"/>
    /// so the binding options reflect that state tools are active. Idempotent: a
    /// repeated call does not register a second module.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddStateTools(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Reflect the opt-in on the binding options so the capability report and
        // any host inspection agree with the registered module set.
        services.Configure<LatticeApiMcpOptions>(static options => options.EnableStateTools = true);

        // TryAddEnumerable dedupes on the implementation type, so repeated calls
        // contribute exactly one state tool group.
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, StateToolGroup>());

        return services;
    }
}
