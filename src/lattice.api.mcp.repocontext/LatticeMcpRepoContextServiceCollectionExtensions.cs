using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The repository-context tool-module opt-in for the <c>Orleans.Lattice.Api.Mcp</c>
/// binding. Adds the MCP tool group that serves an AI agent a durable,
/// conflict-free store of structural facts, notes, and working memory about a
/// codebase.
/// </summary>
/// <remarks>
/// <para>Registered as a companion to <c>AddLatticeMcp</c>:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
/// builder.Services.AddRepoContextTools();
/// // ...
/// app.MapLatticeMcp();
/// </code>
/// <para>
/// The permission-aware discovery core advertises the module's tools only to a
/// caller granted the repository-context group (the same data read-or-write mask
/// that makes the data group usable), and the fail-closed authorization gate
/// enforces the verdict at both advertisement and invocation - this module adds
/// no authorization path of its own. The host must also have called
/// <c>AddLatticeMcp</c> for the tools to be reachable.
/// </para>
/// </remarks>
public static class LatticeMcpRepoContextServiceCollectionExtensions
{
    /// <summary>
    /// Opts the repository-context surface into the MCP binding: registers the
    /// repository-context tool group so its tools are advertised to a caller
    /// holding a data read-or-write grant. Idempotent: calling it more than once
    /// registers exactly one tool group. The host must also have called
    /// <c>AddLatticeMcp</c> for the tools to be reachable.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRepoContextTools(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, RepoContextToolGroup>());

        // Bind the per-repository TTL policy under the named-options convention
        // (IOptionsMonitor<RepoContextTtlOptions>.Get(repoId)) and validate every
        // instance at first resolve. The memory-writing tools consume these.
        services.AddOptions<RepoContextTtlOptions>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<RepoContextTtlOptions>, RepoContextTtlOptionsValidator>());

        return services;
    }
}
