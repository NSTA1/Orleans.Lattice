using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// DI extensions for wiring up the <c>Orleans.Lattice.Api.Data.Grpc</c>
/// binding - the gRPC surface over the write-capable data-API facade.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is two calls. In the host's service composition:</para>
/// <code>
/// builder.Services.AddLatticeDataApiGrpc(o => o.RequireAuthorization = true);
/// builder.Services.AddSingleton&lt;ILatticeDataApiAuthorizer, MyTokenAuthorizer&gt;();
/// </code>
/// <para>And, in the ASP.NET Core endpoint composition:</para>
/// <code>
/// app.MapLatticeDataApiGrpc();
/// </code>
/// <para>
/// The host must also expose the facade (<c>ILatticeDataApi</c>) in the same
/// service provider - typically by co-hosting Orleans with
/// <c>AddLattice(...).AddLatticeDataApi()</c> on the same host. The binding
/// fails closed: with the default <see cref="DenyAllDataApiAuthorizer"/> and
/// <see cref="LatticeDataApiGrpcOptions.RequireAuthorization"/> at its
/// <see langword="true"/> default, every call is rejected until the host opts in
/// via a permissive authorizer or by turning enforcement off.
/// </para>
/// </remarks>
public static class LatticeDataApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <c>Orleans.Lattice.Api.Data.Grpc</c> binding: the
    /// method-definition singleton, the server-side service, the default-deny
    /// authorizer, the identity bridge, and the authorization interceptor.
    /// Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeDataApiGrpcOptions"/>.
    /// </param>
    public static IServiceCollection AddLatticeDataApiGrpc(
        this IServiceCollection services,
        Action<LatticeDataApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeDataApiGrpcOptions>();
        }

        RegisterMethodFactory(services);

        // Default-deny authorizer. TryAdd preserves any permissive authorizer
        // the host registered first; the fallback keeps the surface closed.
        services.TryAddSingleton<ILatticeDataApiAuthorizer, DenyAllDataApiAuthorizer>();

        // Identity bridge: lifts the inbound gRPC credential header into the
        // ambient Lattice credential so the access gate can resolve the caller's
        // subject. TryAdd preserves a host-supplied bridge (for a bespoke
        // identity source such as a client certificate).
        services.TryAddSingleton<ILatticeDataApiCredentialBridge, HeaderLatticeDataApiCredentialBridge>();

        // Active-tenant bridge: lifts the inbound gRPC active-tenant header onto
        // the ambient LatticeActiveTenantContext so the tenant-aware data plane
        // (per-tenant write admission / quota and tenant-scoped tree resolution)
        // sees the caller's asserted tenant, which the tenancy add-on re-validates
        // against the caller's membership downstream. TryAdd preserves a
        // host-supplied bridge (for a bespoke tenant source such as a principal
        // claim).
        services.TryAddSingleton<ILatticeDataApiActiveTenantBridge, HeaderLatticeDataApiActiveTenantBridge>();

        // Register the auth interceptor globally; it scopes enforcement to the
        // data-API service by service-name prefix so unrelated gRPC services on
        // the same host are unaffected.
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<LatticeDataApiGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeDataApiGrpcAuthInterceptor>();

        services.TryAddSingleton<LatticeDataApiGrpcService>();
        services.TryAddSingleton<LatticeDataApiGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeDataApiGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps the data-API RPC routes (the six unary write / read RPCs) on the
    /// supplied <paramref name="endpoints"/>. The host must have called
    /// <see cref="AddLatticeDataApiGrpc"/> and must expose
    /// <c>ILatticeDataApi</c> (via <c>AddLatticeDataApi</c>) in the same service
    /// provider before this call.
    /// </summary>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapLatticeDataApiGrpc(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates the static
        // holder before Grpc.AspNetCore reflects [BindServiceMethod] and invokes
        // the static BindService callback at startup. MapGrpcService targets the
        // abstract base because it bears the attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeDataApiGrpcMethods>();
        endpoints.MapGrpcService<LatticeDataApiGrpcServiceBase>();

        return endpoints;
    }

    private static void RegisterMethodFactory(IServiceCollection services)
    {
        services.TryAddSingleton<LatticeDataApiGrpcMethods>(sp =>
        {
            var methods = LatticeDataApiGrpcMethods.FromServiceProvider(sp);
            LatticeDataApiGrpcMethodsHolder.Current = methods;
            return methods;
        });
    }
}
