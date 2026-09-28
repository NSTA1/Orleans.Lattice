using Grpc.AspNetCore.Server.Model;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Apps.Grpc;

public static partial class LatticeAppsApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the app bridge gRPC service (<c>orleans.lattice.api.apps.bridge</c>) behind the binding's
    /// default-deny authorizer, together with the shared binding infrastructure
    /// <see cref="AddLatticeAppsApiGrpc"/> registers. A host that relays app UI data requests supplies an
    /// <see cref="ILatticeAppsApiAuthorizer"/> admitting the bridge operations; the facade still authorizes every
    /// call against the caller's app-owned role grants and own rights. Every bridge message is bounded per method.
    /// Supply Orleans serialization and an <see cref="ILatticeAppBridge"/> implementation separately. Idempotent.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional binding configuration, shared with the app-control service.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    public static IServiceCollection AddLatticeAppBridgeApiGrpc(
        this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddLatticeAppsApiGrpc(configure);
        services.TryAddSingleton<LatticeAppBridgeGrpcMethods>();
        services.TryAddScoped<LatticeAppBridgeGrpcService>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IServiceMethodProvider<LatticeAppBridgeGrpcService>, LatticeAppBridgeGrpcServiceMethodProvider>());
        services.AddGrpc().AddServiceOptions<LatticeAppBridgeGrpcService>(options =>
        {
            if (!options.Interceptors.Any(i => i.Type == typeof(LatticeAppsApiGrpcAuthInterceptor)))
                options.Interceptors.Add<LatticeAppsApiGrpcAuthInterceptor>();
        });
        return services;
    }

    /// <summary>
    /// Maps every app bridge RPC. Requires <see cref="AddLatticeAppBridgeApiGrpc"/> and an
    /// <see cref="ILatticeAppBridge"/> registration.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>The same <paramref name="endpoints"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is null.</exception>
    public static IEndpointRouteBuilder MapLatticeAppBridgeApiGrpc(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        endpoints.MapGrpcService<LatticeAppBridgeGrpcService>();
        return endpoints;
    }
}
