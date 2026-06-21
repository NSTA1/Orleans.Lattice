using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// DI extensions for wiring up the <c>Orleans.Lattice.Api.State.Grpc</c>
/// binding - the gRPC surface over the read-only state-API facade.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is two calls. In the host's service composition:</para>
/// <code>
/// builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = true);
/// builder.Services.AddSingleton&lt;ILatticeStateApiAuthorizer, MyTokenAuthorizer&gt;();
/// </code>
/// <para>And, in the ASP.NET Core endpoint composition:</para>
/// <code>
/// app.MapLatticeStateApiGrpc();
/// </code>
/// <para>
/// The host must also expose the facade (<c>ILatticeStateQuery</c>) in the
/// same service provider - typically by co-hosting Orleans with
/// <c>AddLattice(...).AddLatticeStateApi()</c> on the same host. The binding
/// fails closed: with the default <see cref="DenyAllStateApiAuthorizer"/> and
/// <see cref="LatticeStateApiGrpcOptions.RequireAuthorization"/> at its
/// <see langword="true"/> default, every call is rejected until the host opts
/// in via a permissive authorizer or by turning enforcement off.
/// </para>
/// </remarks>
public static class LatticeStateApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <c>Orleans.Lattice.Api.State.Grpc</c> binding: the
    /// method-definition singleton, the server-side service, the
    /// default-deny authorizer, and the authorization interceptor. Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeStateApiGrpcOptions"/>.
    /// </param>
    public static IServiceCollection AddLatticeStateApiGrpc(
        this IServiceCollection services,
        Action<LatticeStateApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeStateApiGrpcOptions>();
        }

        RegisterMethodFactory(services);

        // Default-deny authorizer. TryAdd preserves any permissive authorizer
        // the host registered first; the fallback keeps the surface closed.
        services.TryAddSingleton<ILatticeStateApiAuthorizer, DenyAllStateApiAuthorizer>();

        // Register the auth interceptor globally; it scopes enforcement to the
        // state-API service by service-name prefix so unrelated gRPC services
        // on the same host are unaffected.
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<LatticeStateApiGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeStateApiGrpcAuthInterceptor>();

        services.TryAddSingleton<LatticeStateGrpcService>();
        services.TryAddSingleton<LatticeStateGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeStateGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps the five state-API unary RPC routes on the supplied
    /// <paramref name="endpoints"/>. The host must have called
    /// <see cref="AddLatticeStateApiGrpc"/> and must expose
    /// <c>ILatticeStateQuery</c> (via <c>AddLatticeStateApi</c>) in the same
    /// service provider before this call.
    /// </summary>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapLatticeStateApiGrpc(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates the static
        // holder before Grpc.AspNetCore reflects [BindServiceMethod] and
        // invokes the static BindService callback at startup. MapGrpcService
        // targets the abstract base because it bears the attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeStateGrpcMethods>();
        endpoints.MapGrpcService<LatticeStateGrpcServiceBase>();

        return endpoints;
    }

    private static void RegisterMethodFactory(IServiceCollection services)
    {
        services.TryAddSingleton<LatticeStateGrpcMethods>(sp =>
        {
            var methods = new LatticeStateGrpcMethods(
                sp.GetRequiredService<Serializer<CatalogRequest>>(),
                sp.GetRequiredService<Serializer<TreeCatalogPage>>(),
                sp.GetRequiredService<Serializer<ViewCatalogPage>>(),
                sp.GetRequiredService<Serializer<StructureRequest>>(),
                sp.GetRequiredService<Serializer<StructureResponse>>(),
                sp.GetRequiredService<Serializer<EntryScanRequest>>(),
                sp.GetRequiredService<Serializer<EntryScanResponse>>(),
                sp.GetRequiredService<Serializer<EntryGetRequest>>(),
                sp.GetRequiredService<Serializer<EntryGetResponse>>());
            LatticeStateGrpcMethodsHolder.Current = methods;
            return methods;
        });
    }
}
