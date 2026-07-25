using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using ModelContextProtocol.AspNetCore;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extensions for wiring up the <c>Orleans.Lattice.Api.Mcp</c> binding - the
/// Model Context Protocol server surface over the transport-agnostic API
/// facades.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is two calls. In the host's service composition:</para>
/// <code>
/// builder.Services.AddLatticeMcp(o => o.RequireAuthorization = true);
/// builder.Services.AddSingleton&lt;ILatticeApiMcpAuthorizer, MyAuthorizer&gt;();
/// </code>
/// <para>And, in the ASP.NET Core endpoint composition:</para>
/// <code>
/// app.MapLatticeMcp();
/// </code>
/// <para>
/// The host co-hosts the MCP server on the same silo that exposes the facades.
/// The binding fails closed: with the default <see cref="DenyAllMcpAuthorizer"/>,
/// the fail-closed credential bridge, and
/// <see cref="LatticeApiMcpOptions.RequireAuthorization"/> at its
/// <see langword="true"/> default, an unauthenticated session is default-denied
/// and can enumerate or call nothing. The skeleton registers <b>no</b> tools;
/// per-facade tool modules are added separately.
/// </para>
/// </remarks>
public static partial class LatticeMcpServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <c>Orleans.Lattice.Api.Mcp</c> binding: the MCP server with
    /// its streamable-HTTP transport (exposing no tools), the default-deny
    /// authorizer, the fail-closed credential bridge, and the HTTP context
    /// accessor the credential bridge and tool modules read. Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeApiMcpOptions"/>.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddLatticeMcp(
        this IServiceCollection services,
        Action<LatticeApiMcpOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeApiMcpOptions>();
        }

        // Default-deny authorizer. TryAdd preserves any permissive authorizer the
        // host registered first; the fallback keeps the surface closed.
        services.TryAddSingleton<ILatticeApiMcpAuthorizer, DenyAllMcpAuthorizer>();

        // Identity bridge: lifts the authenticated MCP session principal into the
        // ambient Lattice credential so the access gate can resolve the caller's
        // subject. TryAdd preserves a host-supplied bridge (for a bespoke
        // identity source such as a client certificate).
        services.TryAddSingleton<ILatticeApiMcpCredentialBridge, HttpContextLatticeApiMcpCredentialBridge>();

        // The credential bridge and tool modules read the ambient HTTP context.
        services.AddHttpContextAccessor();

        // OAuth 2.0 Protected Resource Metadata (RFC 9728) challenge hint. Inert
        // unless the host sets LatticeApiMcpOptions.ProtectedResourceMetadata.
        AddProtectedResourceMetadataChallenge(services);

        // Permission-aware discovery core: resolves each caller's usable facade
        // groups (default: via the Api.Auth effective-permissions surface) and
        // the per-session configurator that scopes the advertised tool set and
        // the capabilities meta-tool to those grants.
        services.TryAddSingleton<ILatticeApiMcpPermissionResolver, AuthAdminMcpPermissionResolver>();
        services.TryAddSingleton<LatticeApiMcpSessionConfigurator>();

        // Region routing/discovery. The in-silo topology has a single current
        // region co-hosting every registered facade group; the router is built
        // lazily from the registered tool-group set (populated after this call) so
        // the current region's per-group availability matches exactly what is
        // served. The remote-host binding replaces this router with a multi-region
        // one built from its configured peers. The catalog is topology-agnostic and
        // projects whichever router is registered, so discovery and routing share a
        // single source of truth.
        services.TryAddSingleton<ILatticeApiMcpRegionRouter>(BuildInSiloRegionRouter);
        services.TryAddSingleton<Orleans.Lattice.Api.Region.ILatticeRegionCatalog, LatticeApiMcpRegionCatalog>();

        // MCP server over the streamable-HTTP transport. No tools are registered
        // here; per-facade tool modules attach separately.
        services.AddMcpServer().WithHttpTransport();

        // Bridge the binding's stateless toggle onto the transport options so the
        // configured value applies without re-invoking the caller's delegate.
        services.AddOptions<HttpServerTransportOptions>()
            .Configure<IOptions<LatticeApiMcpOptions>>(
                (transport, lattice) => transport.Stateless = lattice.Value.Stateless);

        // Install the per-session discovery hook so every initialised session has
        // its tool set filtered, its capabilities meta-tool installed, and its
        // instructions populated from the caller's permission-scoped view.
        services.AddOptions<HttpServerTransportOptions>()
            .Configure<LatticeApiMcpSessionConfigurator>(
                (transport, configurator) => transport.ConfigureSessionOptions = configurator.ConfigureAsync);

        return services;
    }

    /// <summary>
    /// Maps the MCP streamable-HTTP transport endpoint on the supplied
    /// <paramref name="endpoints"/>, at
    /// <see cref="LatticeApiMcpOptions.TransportPattern"/>. When
    /// <see cref="LatticeApiMcpOptions.RequireAuthorization"/> is
    /// <see langword="true"/> (the default), the endpoint requires an
    /// authenticated caller so an anonymous session is default-denied. The host
    /// must have called <see cref="AddLatticeMcp"/> in the same service provider
    /// before this call.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder to map onto.</param>
    /// <returns>The endpoint convention builder for the mapped MCP endpoint.</returns>
    public static IEndpointConventionBuilder MapLatticeMcp(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var options = endpoints.ServiceProvider
            .GetRequiredService<IOptions<LatticeApiMcpOptions>>().Value;

        var builder = endpoints.MapMcp(options.TransportPattern);
        if (options.RequireAuthorization)
        {
            builder.RequireAuthorization();
        }

        // Serve the anonymous OAuth protected-resource metadata document when the
        // host opts in (no-op otherwise).
        endpoints.MapProtectedResourceMetadata(options);

        return builder;
    }

    private static LatticeApiMcpRegionRouter BuildInSiloRegionRouter(IServiceProvider services)
    {
        // Every co-hosted facade group is served by the single current region at a
        // null (in-process) endpoint. The cluster id is resolved lazily by the
        // catalog at read time, so it is left empty here.
        var groups = new Dictionary<LatticeApiMcpGroup, string?>();
        foreach (var group in services.GetServices<ILatticeApiMcpToolGroup>())
        {
            groups[group.Group] = null;
        }

        var definition = new LatticeApiMcpRegionDefinition
        {
            RegionId = LatticeApiMcpRemoteOptions.DefaultRegionId,
            ClusterId = string.Empty,
            IsCurrent = true,
            Groups = groups,
        };

        return new LatticeApiMcpRegionRouter(
            LatticeApiMcpRemoteOptions.DefaultRegionId,
            new[] { definition });
    }
}
