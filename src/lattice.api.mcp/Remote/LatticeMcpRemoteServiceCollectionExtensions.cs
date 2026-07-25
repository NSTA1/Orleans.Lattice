using System.Collections.Frozen;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// DI extensions for the <c>Orleans.Lattice.Api.Mcp</c> <b>remote-host</b>
/// topology: an MCP server that fronts a cluster it is <b>not</b> co-hosted with
/// by delegating each facade group to its gRPC client. The tool modules are
/// topology-agnostic, so the same discovery, permission-scoping, and fail-closed
/// semantics apply whether the facades are in-process or reached over gRPC.
/// </summary>
public static class LatticeMcpRemoteServiceCollectionExtensions
{
    /// <summary>
    /// Registers the remote-host MCP binding: the base MCP server
    /// (via <see cref="LatticeMcpServiceCollectionExtensions.AddLatticeMcp"/>),
    /// Orleans serialization for the gRPC wire marshallers, the caller-credential
    /// forwarding infrastructure, the per-group endpoint source that feeds the
    /// capabilities report, and - for each configured group - the gRPC-backed
    /// facade adapter plus its tool module. A group with no configured endpoint is
    /// not wired and is reported unavailable with a <see langword="null"/>
    /// endpoint, exactly as in the in-silo topology.
    /// </summary>
    /// <remarks>
    /// Discovery grants a caller a group only when the auth control plane can
    /// report the caller's effective permissions, so
    /// <see cref="LatticeApiMcpRemoteOptions.Auth"/> must be configured (and,
    /// unless the caller is itself an administrator on the remote cluster,
    /// <see cref="LatticeApiMcpRemoteOptions.AdministratorCredential"/> supplied)
    /// for any group's tools to be discovered.
    /// </remarks>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">Delegate that populates <see cref="LatticeApiMcpRemoteOptions"/>. Must not be <c>null</c>.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="configure"/> is <c>null</c>.</exception>
    public static IServiceCollection AddLatticeMcpRemote(
        this IServiceCollection services,
        Action<LatticeApiMcpRemoteOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // Materialise the options once to decide which groups to wire, then persist
        // the same delegate so runtime collaborators (credential source, endpoint
        // source) resolve the identical configuration.
        var options = new LatticeApiMcpRemoteOptions();
        configure(options);

        // Base MCP binding (idempotent) and remote options.
        services.AddLatticeMcp();
        services.Configure(configure);

        // Replace the in-silo single-region router with the multi-region router
        // built from the configured default region plus each peer. Built eagerly
        // from the materialised options - the region set is static configuration -
        // so both discovery (via the catalog) and per-call routing derive from this
        // one source of truth and can never disagree.
        var definitions = BuildRegionDefinitions(options);
        services.Replace(ServiceDescriptor.Singleton<ILatticeApiMcpRegionRouter>(
            new LatticeApiMcpRegionRouter(options.RegionId, definitions)));

        // Orleans serialization so the gRPC clients can build wire marshallers that
        // match the server. Idempotent.
        services.AddSerializer();

        // Caller-credential forwarding: the source resolves the credential per call
        // and the interceptor stamps it onto every outbound gRPC request. The
        // default administrator source returns the static AdministratorCredential;
        // AddLatticeMcpManagedIdentityAdministrator replaces it with a
        // self-refreshing managed-identity token.
        services.AddHttpContextAccessor();
        services.TryAddSingleton<ILatticeApiMcpAdministratorCredentialSource, StaticAdministratorCredentialSource>();
        services.TryAddSingleton<ILatticeApiMcpRemoteCredentialSource, LatticeApiMcpRemoteCredentialSource>();
        services.TryAddSingleton<LatticeApiMcpCredentialForwardingInterceptor>();

        // Feeds the per-group endpoint into the capabilities report.
        services.TryAddSingleton<ILatticeApiMcpGroupEndpointSource, LatticeApiMcpRemoteGroupEndpointSource>();

        // Defers (omits) the tools whose backing gRPC method is not yet bound so a
        // remote session never advertises a tool that would hard-error on invoke.
        services.TryAddSingleton<ILatticeApiMcpUnsupportedToolSource, LatticeApiMcpRemoteUnsupportedToolSource>();

        if (options.State is { } state)
        {
            services.TryAddSingleton<ILatticeStateQuery>(sp =>
                new GrpcLatticeStateQuery(LatticeStateApiGrpcClient.Create(
                    BuildRoutingInvoker(sp, options, state, static r => r.State), sp)));
            services.AddStateTools();
        }

        if (options.Data is { } data)
        {
            services.TryAddSingleton<ILatticeDataApi>(sp =>
                new GrpcLatticeDataApi(LatticeDataApiGrpcClient.Create(
                    BuildRoutingInvoker(sp, options, data, static r => r.Data), sp)));
            services.AddDataTools(options.EnableDataWrites);
        }

        if (options.Auth is { } auth)
        {
            services.TryAddSingleton<ILatticeAuthAdmin>(sp =>
                new GrpcLatticeAuthAdmin(LatticeAuthApiGrpcClient.Create(
                    BuildRoutingInvoker(sp, options, auth, static r => r.Auth), sp)));
            services.AddAuthTools(options.EnableAuthAdministration);
        }

        if (options.Backup is { } backup)
        {
            services.TryAddSingleton<ILatticeBackupControl>(sp =>
                new GrpcLatticeBackupControl(LatticeBackupApiGrpcClient.Create(
                    BuildRoutingInvoker(sp, options, backup, static r => r.Backup), sp)));
            services.AddBackupTools(options.EnableBackupControl);
        }

        if (options.Replication is { } replication)
        {
            services.TryAddSingleton<ILatticeReplicationControl>(sp =>
                new GrpcLatticeReplicationControl(LatticeReplicationApiGrpcClient.Create(
                    BuildRoutingInvoker(sp, options, replication, static r => r.Replication), sp)));
            services.AddReplicationTools(options.EnableReplicationControl);
        }

        return services;
    }

    /// <summary>
    /// Builds a region-routing invoker for one facade group: the default region's
    /// channel plus every peer that serves the group, keyed by region id, wrapped
    /// once with the caller-credential-forwarding interceptor. Each region's raw
    /// channel is built exactly once here (the invoker lives in the group facade's
    /// singleton), never per call; a call with no region selected takes the routing
    /// invoker's cached default-channel branch, adding no allocation versus the
    /// single-region binding. The same forwarded caller credential flows to
    /// whichever region is selected, so the target authorizes independently,
    /// fail-closed.
    /// </summary>
    private static CallInvoker BuildRoutingInvoker(
        IServiceProvider services,
        LatticeApiMcpRemoteOptions options,
        LatticeApiMcpRemoteEndpoint defaultEndpoint,
        Func<LatticeApiMcpRemoteRegionOptions, LatticeApiMcpRemoteEndpoint?> regionEndpointSelector)
    {
        var interceptor = services.GetRequiredService<LatticeApiMcpCredentialForwardingInterceptor>();
        var defaultInvoker = RawInvoker(defaultEndpoint);

        var byRegion = new Dictionary<string, CallInvoker>(StringComparer.Ordinal)
        {
            [options.RegionId] = defaultInvoker,
        };

        foreach (var region in options.Regions)
        {
            var endpoint = regionEndpointSelector(region);
            if (endpoint is not null)
            {
                byRegion[region.RegionId] = RawInvoker(endpoint);
            }
        }

        var routing = new RegionRoutingCallInvoker(
            defaultInvoker,
            byRegion.ToFrozenDictionary(StringComparer.Ordinal));
        return routing.Intercept(interceptor);
    }

    private static CallInvoker RawInvoker(LatticeApiMcpRemoteEndpoint endpoint)
        => endpoint.CallInvoker ?? GrpcChannel.ForAddress(endpoint.Endpoint).CreateCallInvoker();

    private static IReadOnlyList<LatticeApiMcpRegionDefinition> BuildRegionDefinitions(
        LatticeApiMcpRemoteOptions options)
    {
        var definitions = new List<LatticeApiMcpRegionDefinition>(1 + options.Regions.Count)
        {
            new()
            {
                RegionId = options.RegionId,
                ClusterId = options.ClusterId ?? string.Empty,
                IsCurrent = true,
                Groups = GroupEndpoints(
                    options.State, options.Data, options.Backup, options.Auth, options.Replication),
            },
        };

        foreach (var region in options.Regions)
        {
            definitions.Add(new LatticeApiMcpRegionDefinition
            {
                RegionId = region.RegionId,
                ClusterId = region.ClusterId ?? string.Empty,
                IsCurrent = false,
                Groups = GroupEndpoints(
                    region.State, region.Data, region.Backup, region.Auth, region.Replication),
            });
        }

        return definitions;
    }

    private static IReadOnlyDictionary<LatticeApiMcpGroup, string?> GroupEndpoints(
        LatticeApiMcpRemoteEndpoint? state,
        LatticeApiMcpRemoteEndpoint? data,
        LatticeApiMcpRemoteEndpoint? backup,
        LatticeApiMcpRemoteEndpoint? auth,
        LatticeApiMcpRemoteEndpoint? replication)
    {
        var groups = new Dictionary<LatticeApiMcpGroup, string?>();
        if (state is not null)
        {
            groups[LatticeApiMcpGroup.State] = state.Endpoint;
        }

        if (data is not null)
        {
            groups[LatticeApiMcpGroup.Data] = data.Endpoint;
        }

        if (backup is not null)
        {
            groups[LatticeApiMcpGroup.Backup] = backup.Endpoint;
        }

        if (auth is not null)
        {
            groups[LatticeApiMcpGroup.Auth] = auth.Endpoint;
        }

        if (replication is not null)
        {
            groups[LatticeApiMcpGroup.Replication] = replication.Endpoint;
        }

        return groups;
    }
}
