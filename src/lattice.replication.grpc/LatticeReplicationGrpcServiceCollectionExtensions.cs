using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// DI extensions for wiring up the unified
/// <c>Orleans.Lattice.Replication.Grpc</c> binding (live push + snapshot
/// bootstrap, sender + receiver) on a silo.
/// </summary>
/// <remarks>
/// <para>
/// The canonical wiring is two calls. In the silo composition root:
/// </para>
/// <code>
/// siloBuilder.AddLatticeReplication(opts => opts.ClusterId = "site-b");
/// siloBuilder.Services.AddLatticeReplicationGrpc(grpc =>
/// {
///     grpc.Peers["site-a"] = new Uri("https://site-a.example/");
/// });
/// </code>
/// <para>
/// And, in the ASP.NET Core endpoint composition:
/// </para>
/// <code>
/// app.MapLatticeReplicationGrpc();
/// </code>
/// <para>
/// <see cref="AddLatticeReplicationGrpc"/> registers both transports
/// (live-push client + server, snapshot client + server), the shared
/// auth interceptor, and the secret-provider chain.
/// <see cref="MapLatticeReplicationGrpc"/> maps both the live-push
/// route and the snapshot routes on the endpoint builder. Active-active
/// is the zero-ceremony default: a silo that registers the binding is
/// both a sender (peer receivers can pull live pushes and snapshot
/// streams from it) and a receiver (the silo can dial peer endpoints
/// listed in <see cref="LatticeReplicationGrpcOptions.Peers"/> to ship
/// outbound batches and to bootstrap from a peer).
/// </para>
/// <para>
/// Push-only deployments (a silo that ships outbound but never expects
/// peers to dial it) omit the endpoint-mapping call;
/// receiver-only deployments (a silo that accepts inbound pushes /
/// snapshot pulls but never bootstraps from a peer) leave
/// <see cref="LatticeReplicationGrpcOptions.Peers"/> empty. The
/// composition is registration-driven, not role-flag-driven.
/// </para>
/// </remarks>
public static class LatticeReplicationGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the unified <c>Orleans.Lattice.Replication.Grpc</c>
    /// binding. Wires the live-push client + server, the snapshot
    /// client + server, the shared-secret auth interceptor, and the
    /// secret-provider chain in a single call. Idempotent: a host
    /// that calls this more than once layers the supplied
    /// <paramref name="configure"/> delegate over the existing
    /// options binding rather than re-registering singletons.
    /// </summary>
    /// <param name="services">The silo's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates the unified
    /// <see cref="LatticeReplicationGrpcOptions"/>. Omit when this
    /// silo is receiver-only and never dials peer endpoints.
    /// </param>
    /// <remarks>
    /// Call after <c>AddLatticeReplication</c>. The replacement of
    /// the no-op <see cref="IReplicationTransport"/> uses
    /// <see cref="ServiceCollectionDescriptorExtensions.Replace"/>
    /// so the no-op singleton registered earlier is removed before
    /// the gRPC transport is added; the snapshot transport uses the
    /// same pattern. Subsequent calls are idempotent and do not
    /// stack additional transports.
    /// </remarks>
    public static IServiceCollection AddLatticeReplicationGrpc(
        this IServiceCollection services,
        Action<LatticeReplicationGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeReplicationGrpcOptions>();
        }

        // Project the unified public options onto the two internal
        // per-transport options instances. The projection runs every
        // time the unified options are reloaded, so an IOptionsMonitor
        // reload propagates to both transports without per-transport
        // wiring on the host's side.
        services.AddSingleton<IConfigureOptions<GrpcPushTransportOptions>>(sp =>
            new ProjectPushOptions(sp.GetRequiredService<IOptionsMonitor<LatticeReplicationGrpcOptions>>()));
        services.AddSingleton<IConfigureOptions<GrpcRemoteSnapshotTransportOptions>>(sp =>
            new ProjectSnapshotOptions(sp.GetRequiredService<IOptionsMonitor<LatticeReplicationGrpcOptions>>()));

        // Common security defaults. TryAdd preserves any registration
        // the host did first (typically AddLatticeReplication has
        // already supplied these); the entries are a safe fallback
        // for a stand-alone gRPC host (e.g. a test that wires only
        // the gRPC surface against a stub applier).
        services.TryAddSingleton<ILatticeReplicationSecretSource, EnvironmentVariableSecretSource>();
        services.TryAddSingleton<TimeProvider>(_ => TimeProvider.System);
        services.TryAddSingleton<IReplicationSecretProvider, CachingReplicationSecretProvider>();
        services.AddOptions<LatticeReplicationSecurityOptions>();
        services.AddOptions<LatticeReplicationOptions>();

        // The framing-aware push marshaller decodes pre-encoded WAL
        // entry bytes back into WalRecord instances on the receiver
        // side via IWalRecordEncoder. Hosts that call AddLattice
        // already register the canonical Orleans-binary encoder; the
        // TryAdd here is the safe fallback for stand-alone gRPC
        // hosts (typically integration tests) that compose only the
        // replication-grpc surface.
        services.TryAddSingleton<IWalRecordEncoder, OrleansBinaryWalRecordEncoder>();

        // Register the auth interceptor globally so every gRPC
        // service hosted on this pipeline runs through the
        // shared-secret authenticator. The interceptor itself
        // scopes enforcement to LatticeReplication methods by
        // matching on the service-name prefix, so unrelated gRPC
        // services on the same host are unaffected.
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<LatticeReplicationGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeReplicationGrpcAuthInterceptor>();

        // Live-push transport (outbound, client side).
        RegisterPushMethodFactory(services);
        services.Replace(ServiceDescriptor.Singleton<IReplicationTransport, GrpcPushTransport>());

        // Live-push receiver service (inbound, server side).
        services.TryAddSingleton<IWalCursorRegistry, InMemoryWalCursorRegistry>();
        // Default receiver-side flow-control policy. Hosts that wire
        // a custom policy via AddLatticeReplication retain their
        // registration because both call sites use TryAddSingleton;
        // hosts that wire the gRPC stack without AddLatticeReplication
        // (e.g. integration tests that compose pieces directly) get
        // the no-op default and ack with null hint slots.
        services.TryAddSingleton<IReceiverFlowControlPolicy>(_ => NoOpReceiverFlowControlPolicy.Instance);
        services.TryAddSingleton<LatticeReplicationGrpcService>();
        services.TryAddSingleton<LatticeReplicationGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeReplicationGrpcService>());

        // Snapshot transport (outbound, client side).
        RegisterSnapshotMethodFactory(services);
        services.Replace(ServiceDescriptor.Singleton<IRemoteSnapshotTransport, GrpcRemoteSnapshotTransport>());

        // Snapshot sender service (inbound, server side).
        services.TryAddSingleton<LatticeRemoteSnapshotService>();
        services.TryAddSingleton<LatticeRemoteSnapshotGrpcService>();
        services.TryAddSingleton<LatticeRemoteSnapshotGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeRemoteSnapshotGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps both the live-push <c>Push</c> route and the snapshot
    /// <c>GetMetadata</c>/<c>RequestSnapshot</c> routes on the
    /// supplied <paramref name="endpoints"/>. The host must have
    /// called <c>AddLatticeReplication</c> (for the
    /// <see cref="IReplicationApplier"/> + encoder dependencies) and
    /// <see cref="AddLatticeReplicationGrpc"/> before this call.
    /// </summary>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapLatticeReplicationGrpc(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singletons so their factories populate
        // the static method holders before Grpc.AspNetCore reflects
        // [BindServiceMethod] and invokes the static BindService
        // callback at startup. MapGrpcService targets the abstract base
        // classes because those are the types bearing the
        // [BindServiceMethod] attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeReplicationGrpcMethod>();
        endpoints.MapGrpcService<LatticeReplicationGrpcServiceBase>();

        endpoints.ServiceProvider.GetRequiredService<LatticeRemoteSnapshotGrpcMethods>();
        endpoints.MapGrpcService<LatticeRemoteSnapshotGrpcServiceBase>();

        return endpoints;
    }

    private static void RegisterPushMethodFactory(IServiceCollection services)
    {
        // Singleton factory bridges the DI-resolved Method<,> into the
        // static LatticeReplicationGrpcMethodHolder, because the static
        // BindService callback that Grpc.AspNetCore invokes at startup
        // cannot accept DI dependencies directly.
        services.TryAddSingleton<LatticeReplicationGrpcMethod>(sp =>
        {
            var encoder = sp.GetRequiredService<IReplicationBatchEncoder>();
            var walRecordEncoder = sp.GetRequiredService<IWalRecordEncoder>();
            var ackSerializer = sp.GetRequiredService<Serializer<ReplicationAck>>();
            var method = new LatticeReplicationGrpcMethod(encoder, walRecordEncoder, ackSerializer);
            LatticeReplicationGrpcMethodHolder.Current = method;
            return method;
        });
    }

    private static void RegisterSnapshotMethodFactory(IServiceCollection services)
    {
        services.TryAddSingleton<LatticeRemoteSnapshotGrpcMethods>(sp =>
        {
            var requestSerializer = sp.GetRequiredService<Serializer<RemoteSnapshotMetadataRequest>>();
            var metadataSerializer = sp.GetRequiredService<Serializer<RemoteSnapshotMetadata>>();
            var streamItemSerializer = sp.GetRequiredService<Serializer<RemoteSnapshotStreamItem>>();
            var methods = new LatticeRemoteSnapshotGrpcMethods(requestSerializer, metadataSerializer, streamItemSerializer);
            LatticeRemoteSnapshotGrpcMethodsHolder.Current = methods;
            return methods;
        });
    }

    private sealed class ProjectPushOptions(IOptionsMonitor<LatticeReplicationGrpcOptions> source)
        : IConfigureOptions<GrpcPushTransportOptions>
    {
        public void Configure(GrpcPushTransportOptions options)
        {
            var u = source.CurrentValue;
            foreach (var kvp in u.Peers)
            {
                options.PeerEndpoints[kvp.Key] = kvp.Value;
            }
            options.AllowPlaintextEndpoints = u.AllowPlaintextEndpoints;
            options.ConfigureChannel = u.ConfigureChannel;
            options.LocalClusterId = u.LocalClusterId;
        }
    }

    private sealed class ProjectSnapshotOptions(IOptionsMonitor<LatticeReplicationGrpcOptions> source)
        : IConfigureOptions<GrpcRemoteSnapshotTransportOptions>
    {
        public void Configure(GrpcRemoteSnapshotTransportOptions options)
        {
            var u = source.CurrentValue;
            foreach (var kvp in u.Peers)
            {
                options.SenderEndpoints[kvp.Key] = kvp.Value;
            }
            options.AllowPlaintextEndpoints = u.AllowPlaintextEndpoints;
            options.ConfigureChannel = u.ConfigureChannel;
            options.LocalClusterId = u.LocalClusterId;
        }
    }
}
