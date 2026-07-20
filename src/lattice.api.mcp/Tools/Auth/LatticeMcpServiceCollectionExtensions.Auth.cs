using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The auth-admin tool-module opt-in for the <c>Orleans.Lattice.Api.Mcp</c>
/// binding. Adds the MCP tools that adapt the <c>ILatticeAuthAdmin</c>
/// control-plane facade.
/// </summary>
public static partial class LatticeMcpServiceCollectionExtensions
{
    /// <summary>
    /// Opts the auth-admin control plane into the MCP surface: registers the auth
    /// tool module so its introspection tools (policy and membership reads) are
    /// advertised to an <b>administrator</b> caller, and - when
    /// <paramref name="enableAdministration"/> is <see langword="true"/> - the
    /// mutating administration verbs (group / membership / rule
    /// upsert-remove) as well. Idempotent. The host must also have called
    /// <see cref="AddLatticeMcp"/> and registered the auth facade for the tools to
    /// be reachable.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The whole group is administrator-gated: the discovery core advertises it
    /// only to a caller whose effective permissions grant the administrator
    /// capability, so a non-administrator session is offered no auth tools at all,
    /// and the facade refuses any invocation from a non-administrator fail-closed.
    /// Enabling administration widens only the <b>advertised</b> tool set; it adds
    /// no authorization path of its own.
    /// </para>
    /// <para>
    /// The mutating verbs are opt-in because they are destructive: by default only
    /// the read-only introspection tools are exposed, so a host can safely surface
    /// policy visibility without also surfacing policy mutation.
    /// </para>
    /// </remarks>
    /// <param name="services">The host's service collection.</param>
    /// <param name="enableAdministration">
    /// Whether to also advertise the mutating administration verbs. Defaults to
    /// <see langword="false"/> (introspection only).
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddAuthTools(
        this IServiceCollection services,
        bool enableAdministration = false)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.Configure<LatticeApiMcpOptions>(options =>
        {
            options.EnableAuthTools = true;
            if (enableAdministration)
            {
                options.EnableAuthAdministration = true;
            }
        });

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILatticeApiMcpToolGroup, AuthToolGroup>());

        return services;
    }
}
