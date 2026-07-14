using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extension for registering the <c>Orleans.Lattice.Api.Mcp</c> data tool
/// module - the per-facade opt-in that contributes MCP tools over the
/// transport-agnostic <c>ILatticeDataApi</c> facade.
/// </summary>
/// <remarks>
/// <para>
/// Registered as a companion to <see cref="LatticeMcpServiceCollectionExtensions.AddLatticeMcp"/>:
/// </para>
/// <code>
/// builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
/// builder.Services.AddDataTools(enableWrites: true);
/// </code>
/// <para>
/// The host must also register the data facade itself
/// (<c>siloBuilder.AddLatticeDataApi(...)</c>) so the tools can resolve
/// <c>ILatticeDataApi</c> from the request service provider. The permission-aware
/// discovery core advertises the module's tools only to callers granted the data
/// group, and the facade enforces the per-key verdict fail-closed at call time -
/// this module adds no authorization path of its own.
/// </para>
/// </remarks>
public static class LatticeMcpDataToolsServiceCollectionExtensions
{
    /// <summary>
    /// Adds the data tool module to the MCP server. Always contributes the two
    /// read tools (<c>data_get</c>, <c>data_read_range</c>); contributes the four
    /// mutating tools (<c>data_set</c>, <c>data_delete</c>,
    /// <c>data_set_many_atomic</c>, <c>data_set_many_atomic_cross_tree</c>) only
    /// when <paramref name="enableWrites"/> is <see langword="true"/>. Call once.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="enableWrites">
    /// Whether the destructive write tools are contributed. Defaults to
    /// <see langword="false"/> so a data-permitted caller is offered read tools
    /// only until the host explicitly opts writes in.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddDataTools(
        this IServiceCollection services,
        bool enableWrites = false)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<ILatticeApiMcpToolGroup>(_ => new DataToolGroup(enableWrites));
        return services;
    }
}
