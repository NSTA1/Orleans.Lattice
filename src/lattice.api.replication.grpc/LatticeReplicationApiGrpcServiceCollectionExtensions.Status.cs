using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Replication.Grpc;

public static partial class LatticeReplicationApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the read-only replication peer-status gRPC service on top of the
    /// binding <see cref="AddLatticeReplicationApiGrpc"/> already registered. The
    /// status service reuses that binding's default-deny authorizer, credential
    /// bridge, options and authorization interceptor unchanged, so it sits behind
    /// the same posture: with the default <see cref="DenyAllReplicationApiAuthorizer"/>
    /// every <c>GetPeerStatus</c> call is rejected until the host opts in. The host
    /// must also expose <see cref="ILatticeReplicationStatus"/> in the same service
    /// provider (via <c>AddLatticeReplicationStatusApi</c>). Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// <see cref="AddLatticeReplicationApiGrpc"/> has not been called on the same collection first.
    /// </exception>
    public static IServiceCollection AddLatticeReplicationStatusApiGrpc(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // The status service has no authorization of its own; it relies on the
        // interceptor, authorizer and credential bridge the control binding
        // registers. Refuse to register it without them rather than expose it.
        if (!services.Any(d => d.ServiceType == typeof(LatticeReplicationApiGrpcAuthInterceptor)))
        {
            throw new InvalidOperationException(
                "AddLatticeReplicationStatusApiGrpc() must be called after AddLatticeReplicationApiGrpc(), which "
                + "registers the authorization interceptor, authorizer and credential bridge the status service "
                + "sits behind.");
        }

        services.TryAddSingleton<LatticeReplicationStatusGrpcMethods>(sp =>
        {
            var methods = LatticeReplicationStatusGrpcMethods.FromServiceProvider(sp);
            LatticeReplicationStatusGrpcMethodsHolder.Current = methods;
            return methods;
        });
        services.TryAddSingleton<LatticeReplicationStatusGrpcService>();
        services.TryAddSingleton<LatticeReplicationStatusGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeReplicationStatusGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps the replication peer-status RPC route (the unary <c>GetPeerStatus</c>
    /// RPC) on <paramref name="endpoints"/>. The host must have called
    /// <see cref="AddLatticeReplicationStatusApiGrpc"/> first.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is <see langword="null"/>.</exception>
    public static IEndpointRouteBuilder MapLatticeReplicationStatusApiGrpc(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates the static
        // holder before Grpc.AspNetCore reflects [BindServiceMethod].
        endpoints.ServiceProvider.GetRequiredService<LatticeReplicationStatusGrpcMethods>();
        endpoints.MapGrpcService<LatticeReplicationStatusGrpcServiceBase>();

        return endpoints;
    }
}
