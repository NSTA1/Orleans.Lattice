using Grpc.AspNetCore.Server.Model;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Registers and maps the single app-control gRPC endpoint.</summary>
public static partial class LatticeAppsApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the binding with default-deny authorization. Supply Orleans serialization
    /// and an ILatticeAppsControl implementation separately. Repeated registration preserves
    /// custom collaborators and does not duplicate the interceptor.
    /// </summary>
    public static IServiceCollection AddLatticeAppsApiGrpc(
        this IServiceCollection services, Action<LatticeAppsApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddOptions<LatticeAppsApiGrpcOptions>();
        if (configure is not null)
            services.Configure(configure);

        services.TryAddSingleton<LatticeAppsGrpcMethods>();
        services.TryAddSingleton<ILatticeAppsApiAuthorizer, DenyAppsApiAuthorizer>();
        services.TryAddSingleton<ILatticeAppsApiCredentialBridge, HeaderLatticeAppsApiCredentialBridge>();
        services.TryAddSingleton<ILatticeAppsApiAuthSchemeSource, OptionsLatticeAppsApiAuthSchemeSource>();
        services.TryAddTransient<LatticeAppsApiGrpcAuthInterceptor>();
        services.TryAddScoped<LatticeAppsGrpcService>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IServiceMethodProvider<LatticeAppsGrpcService>, LatticeAppsGrpcServiceMethodProvider>());
        services.AddGrpc().AddServiceOptions<LatticeAppsGrpcService>(options =>
        {
            if (!options.Interceptors.Any(i => i.Type == typeof(LatticeAppsApiGrpcAuthInterceptor)))
                options.Interceptors.Add<LatticeAppsApiGrpcAuthInterceptor>();
        });
        return services;
    }

    /// <summary>Maps all app-control RPCs. Requires AddLatticeAppsApiGrpc and an ILatticeAppsControl registration.</summary>
    public static IEndpointRouteBuilder MapLatticeAppsApiGrpc(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        endpoints.MapGrpcService<LatticeAppsGrpcService>();
        return endpoints;
    }
}
