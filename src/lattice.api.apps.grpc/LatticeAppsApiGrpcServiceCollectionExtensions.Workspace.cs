using Grpc.AspNetCore.Server.Model;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Apps.Grpc;

public static partial class LatticeAppsApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the app workspace gRPC service (<c>orleans.lattice.api.apps.workspace</c>) behind the binding's
    /// default-deny authorizer, together with the shared binding infrastructure
    /// <see cref="AddLatticeAppsApiGrpc"/> registers. The workspace is a per-user surface, so a host that
    /// serves it supplies an <see cref="ILatticeAppsApiAuthorizer"/> admitting its operations; the facade still
    /// evaluates every call against the caller's app roles. Supply Orleans serialization and an
    /// <see cref="ILatticeAppWorkspace"/> implementation separately. Idempotent.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional binding configuration, shared with the app-control service.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    public static IServiceCollection AddLatticeAppWorkspaceApiGrpc(
        this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddLatticeAppsApiGrpc(configure);
        services.TryAddSingleton<LatticeAppWorkspaceGrpcMethods>();
        services.TryAddScoped<LatticeAppWorkspaceGrpcService>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IServiceMethodProvider<LatticeAppWorkspaceGrpcService>, LatticeAppWorkspaceGrpcServiceMethodProvider>());
        services.AddGrpc().AddServiceOptions<LatticeAppWorkspaceGrpcService>(options =>
        {
            if (!options.Interceptors.Any(i => i.Type == typeof(LatticeAppsApiGrpcAuthInterceptor)))
                options.Interceptors.Add<LatticeAppsApiGrpcAuthInterceptor>();
        });
        return services;
    }

    /// <summary>
    /// Maps every app workspace RPC. Requires <see cref="AddLatticeAppWorkspaceApiGrpc"/> and an
    /// <see cref="ILatticeAppWorkspace"/> registration.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>The same <paramref name="endpoints"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is null.</exception>
    public static IEndpointRouteBuilder MapLatticeAppWorkspaceApiGrpc(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        endpoints.MapGrpcService<LatticeAppWorkspaceGrpcService>();
        return endpoints;
    }
}
