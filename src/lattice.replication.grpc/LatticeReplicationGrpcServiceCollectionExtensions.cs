using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Grpc;

/// <summary>
/// DI extensions for wiring up the gRPC streaming push transport on
/// both the sender (silo) and the receiver (ASP.NET Core host).
/// </summary>
public static class LatticeReplicationGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="GrpcPushTransport"/> as the silo's
    /// <see cref="IReplicationTransport"/>, replacing the no-op
    /// transport <c>AddLatticeReplication</c> registers by default.
    /// Binds the supplied <paramref name="configure"/> delegate to the
    /// unnamed <see cref="GrpcPushTransportOptions"/> instance.
    /// </summary>
    /// <remarks>
    /// Call this after <c>AddLatticeReplication</c>. The replacement
    /// uses <see cref="ServiceCollectionDescriptorExtensions.Replace"/>
    /// so the no-op singleton registered earlier is removed before the
    /// gRPC transport is added; subsequent calls to
    /// <c>AddLatticeReplicationGrpcPushTransport</c> are idempotent and
    /// do not stack additional transports.
    /// </remarks>
    public static IServiceCollection AddLatticeReplicationGrpcPushTransport(
        this IServiceCollection services,
        Action<GrpcPushTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        RegisterMethodFactory(services);

        // The transport now depends on IReplicationSecretProvider and
        // IOptionsMonitor<LatticeReplicationOptions>. AddLatticeReplication
        // is the canonical registration site for both, but a host that
        // wires the gRPC transport without the core seam (e.g. a test
        // that injects a stub applier directly) still needs a working
        // DI graph. TryAdd preserves any registration the host did
        // first and supplies safe defaults otherwise.
        services.TryAddSingleton<ILatticeReplicationSecretSource, EnvironmentVariableSecretSource>();
        services.TryAddSingleton<TimeProvider>(_ => TimeProvider.System);
        services.TryAddSingleton<IReplicationSecretProvider, CachingReplicationSecretProvider>();
        services.AddOptions<LatticeReplicationSecurityOptions>();
        services.AddOptions<LatticeReplicationOptions>();

        services.Replace(ServiceDescriptor.Singleton<IReplicationTransport, GrpcPushTransport>());
        return services;
    }

    /// <summary>
    /// Adds the receiver-side gRPC service to the host's service
    /// collection. Call <see cref="MapLatticeReplicationGrpcService"/>
    /// on the endpoint route builder during request-pipeline
    /// configuration to expose the route.
    /// </summary>
    public static IServiceCollection AddLatticeReplicationGrpcServer(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddGrpc(options =>
        {
            // Register the auth interceptor globally so every gRPC
            // service hosted on this pipeline runs through the
            // shared-secret authenticator. The interceptor itself
            // scopes enforcement to LatticeReplication methods by
            // matching on the service-name prefix, so unrelated
            // gRPC services on the same host are unaffected.
            options.Interceptors.Add<LatticeReplicationGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeReplicationGrpcAuthInterceptor>();

        // Security defaults: TryAdd so a host that called
        // AddLatticeReplication first wins (its registrations land
        // before these), and a stand-alone gRPC server host that
        // doesn't have AddLatticeReplication still gets a complete DI
        // graph for the interceptor. The accepted-set returned by the
        // default EnvironmentVariableSecretSource is empty when no
        // env vars are set, so the interceptor will reject every call
        // with PermissionDenied; tests that don't configure a secret
        // must set LatticeReplicationSecurityOptions.RequireAuthentication
        // to false explicitly.
        services.TryAddSingleton<ILatticeReplicationSecretSource, EnvironmentVariableSecretSource>();
        services.TryAddSingleton<TimeProvider>(_ => TimeProvider.System);
        services.TryAddSingleton<IReplicationSecretProvider, CachingReplicationSecretProvider>();
        services.AddOptions<LatticeReplicationSecurityOptions>();

        RegisterMethodFactory(services);
        // Defensive default: hosts that don't call AddLatticeReplication
        // (e.g. test hosts that wire only the gRPC service against a
        // substituted IReplicationApplier) still need a concrete
        // IWalCursorRegistry for the gRPC service's
        // post-apply blocked-floor read. TryAdd preserves any explicit
        // registration the host already made (production hosts that
        // called AddLatticeReplication land their canonical
        // InMemoryWalCursorRegistry singleton first).
        services.TryAddSingleton<IWalCursorRegistry, InMemoryWalCursorRegistry>();
        services.TryAddSingleton<LatticeReplicationGrpcService>();
        services.TryAddSingleton<LatticeReplicationGrpcServiceBase>(sp => sp.GetRequiredService<LatticeReplicationGrpcService>());
        return services;
    }

    /// <summary>
    /// Maps the receiver-side gRPC <c>Push</c> route on the supplied
    /// <paramref name="endpoints"/>. The host must have called
    /// <c>AddLatticeReplication</c> (for the
    /// <see cref="IReplicationApplier"/> + encoder dependencies) and
    /// <see cref="AddLatticeReplicationGrpcServer"/> before this call.
    /// </summary>
    public static GrpcServiceEndpointConventionBuilder MapLatticeReplicationGrpcService(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates
        // LatticeReplicationGrpcMethodHolder.Current before
        // Grpc.AspNetCore reflects [BindServiceMethod] and invokes the
        // static BindService callback at startup. MapGrpcService
        // targets the abstract base class because that is the type
        // bearing the [BindServiceMethod] attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeReplicationGrpcMethod>();
        return endpoints.MapGrpcService<LatticeReplicationGrpcServiceBase>();
    }

    private static void RegisterMethodFactory(IServiceCollection services)
    {
        // Singleton factory bridges the DI-resolved Method<,> into the
        // static LatticeReplicationGrpcMethodHolder, because the static
        // BindService callback that Grpc.AspNetCore invokes at startup
        // cannot accept DI dependencies directly.
        services.TryAddSingleton<LatticeReplicationGrpcMethod>(sp =>
        {
            var encoder = sp.GetRequiredService<IReplicationBatchEncoder>();
            var ackSerializer = sp.GetRequiredService<Serializer<ReplicationAck>>();
            var method = new LatticeReplicationGrpcMethod(encoder, ackSerializer);
            LatticeReplicationGrpcMethodHolder.Current = method;
            return method;
        });
    }
}
