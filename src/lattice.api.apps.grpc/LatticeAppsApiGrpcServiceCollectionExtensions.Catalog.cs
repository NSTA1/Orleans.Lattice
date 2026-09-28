using Grpc.AspNetCore.Server.Model;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Apps.Grpc;

public static partial class LatticeAppsApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the app catalogue gRPC service (<c>orleans.lattice.api.apps.catalog</c>) behind the binding's
    /// default-deny authorizer, together with the shared binding infrastructure
    /// <see cref="AddLatticeAppsApiGrpc"/> registers. Supply Orleans serialization and an
    /// <see cref="ILatticeAppCatalog"/> implementation separately. Idempotent.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional binding configuration, shared with the app-control service.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    public static IServiceCollection AddLatticeAppCatalogApiGrpc(
        this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddLatticeAppsApiGrpc(configure);
        services.TryAddSingleton<LatticeAppCatalogGrpcMethods>();
        services.TryAddScoped<LatticeAppCatalogGrpcService>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IServiceMethodProvider<LatticeAppCatalogGrpcService>, LatticeAppCatalogGrpcServiceMethodProvider>());
        services.AddGrpc().AddServiceOptions<LatticeAppCatalogGrpcService>(options =>
        {
            if (!options.Interceptors.Any(i => i.Type == typeof(LatticeAppsApiGrpcAuthInterceptor)))
                options.Interceptors.Add<LatticeAppsApiGrpcAuthInterceptor>();
        });
        return services;
    }

    /// <summary>
    /// Maps every app catalogue RPC. Requires <see cref="AddLatticeAppCatalogApiGrpc"/> and an
    /// <see cref="ILatticeAppCatalog"/> registration.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>The same <paramref name="endpoints"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is null.</exception>
    public static IEndpointRouteBuilder MapLatticeAppCatalogApiGrpc(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        endpoints.MapGrpcService<LatticeAppCatalogGrpcService>();
        return endpoints;
    }
}
