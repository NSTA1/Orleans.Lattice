using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp.Apps;

/// <summary>
/// Registers the installable-app MCP tool surface onto a host that already runs the
/// Lattice MCP server.
/// </summary>
/// <remarks>
/// <para>Registered as a companion to <c>AddLatticeMcp</c>, in-silo, next to the app registry:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
/// builder.Services.AddAppMcpTools();
/// builder.Services.AddSingleton&lt;IAppMcpToolProvider&gt;(new AppMcpToolProvider(slug, tools));
/// </code>
/// <para>
/// Each enabled app's tools are advertised as <c>{slug}_{tool}</c>, per caller, through the
/// same default-deny authorizer and per-session tool collection as every other Lattice MCP
/// tool, so the host's <c>ILatticeApiMcpAuthorizer</c> must admit the namespaced names. The
/// surface reads the app registry projection, the app source and the shared access gate
/// from the container; when any of them is missing it offers no app tools. The facade
/// groups and the <c>lattice_capabilities</c> report are unaffected.
/// </para>
/// </remarks>
public static class LatticeMcpAppsServiceCollectionExtensions
{
    /// <summary>
    /// Adds the app MCP tool surface. Idempotent: calling it more than once registers the
    /// surface once.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns><paramref name="services"/>, for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddAppMcpTools(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<AppMcpToolSource>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpAppToolSource, AppMcpToolSource>(
                static sp => sp.GetRequiredService<AppMcpToolSource>()));
        return services;
    }
}
