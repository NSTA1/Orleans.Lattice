using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// DI extensions for wiring up the <c>Orleans.Lattice.Api.TenantAdmin.Grpc</c>
/// binding - the gRPC surface over the tenant-administration control facade.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is two calls. In the host's service composition:</para>
/// <code>
/// builder.Services.AddLatticeTenantAdminApiGrpc(o => o.RequireAuthorization = true);
/// builder.Services.AddSingleton&lt;ILatticeTenantAdminApiAuthorizer, MyTokenAuthorizer&gt;();
/// </code>
/// <para>And, in the ASP.NET Core endpoint composition:</para>
/// <code>
/// app.MapLatticeTenantAdminApiGrpc();
/// </code>
/// <para>
/// The host must also expose the facade
/// (<c>Orleans.Lattice.Api.TenantAdmin.ILatticeTenantAdmin</c>) in the same service
/// provider - typically by co-hosting Orleans with
/// <c>AddLattice(...).AddLatticeTenancy(...).AddLatticeTenantAdminApi()</c> on the
/// same host. The binding fails closed: with the default
/// <see cref="DenyTenantAdminApiAuthorizer"/> and
/// <see cref="LatticeTenantAdminApiGrpcOptions.RequireAuthorization"/> at its
/// <see langword="true"/> default, every call is rejected until the host opts in
/// via a permissive authorizer or by turning enforcement off.
/// </para>
/// </remarks>
public static class LatticeTenantAdminApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <c>Orleans.Lattice.Api.TenantAdmin.Grpc</c> binding: the
    /// method-definition singleton, the server-side service, the default-deny
    /// authorizer, and the authorization interceptor. Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeTenantAdminApiGrpcOptions"/>.
    /// </param>
    /// <returns>The same service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <c>null</c>.</exception>
    public static IServiceCollection AddLatticeTenantAdminApiGrpc(
        this IServiceCollection services,
        Action<LatticeTenantAdminApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeTenantAdminApiGrpcOptions>();
        }

        RegisterMethodFactory(services);

        // Default-deny authorizer. TryAdd preserves any permissive authorizer the
        // host registered first; the fallback keeps the surface closed.
        services.TryAddSingleton<ILatticeTenantAdminApiAuthorizer, DenyTenantAdminApiAuthorizer>();

        // Identity bridge: lifts the inbound gRPC credential header into the ambient
        // Lattice credential so the composed access gate can resolve the caller's
        // subject. TryAdd preserves a host-supplied bridge (for a bespoke identity
        // source such as a client certificate).
        services.TryAddSingleton<ILatticeTenantAdminApiCredentialBridge, HeaderLatticeTenantAdminApiCredentialBridge>();

        // Auth-scheme advertisement source: the unauthenticated GetAuthScheme RPC
        // reads the schemes a host configured via options. TryAdd preserves a
        // host-supplied source. Advertises nothing by default.
        services.TryAddSingleton<ILatticeTenantAdminApiAuthSchemeSource, OptionsLatticeTenantAdminApiAuthSchemeSource>();

        // Register the auth interceptor globally; it scopes enforcement to the
        // tenant-administration control-API service by service-name prefix so
        // unrelated gRPC services on the same host are unaffected.
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<LatticeTenantAdminApiGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeTenantAdminApiGrpcAuthInterceptor>();

        services.TryAddSingleton<LatticeTenantAdminGrpcService>();
        services.TryAddSingleton<LatticeTenantAdminGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeTenantAdminGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps the tenant-administration control-API RPC routes (the four tenant
    /// lifecycle operations and the unauthenticated auth-scheme discovery RPC) on
    /// the supplied <paramref name="endpoints"/>. The host must have called
    /// <see cref="AddLatticeTenantAdminApiGrpc"/> and must expose
    /// <c>Orleans.Lattice.Api.TenantAdmin.ILatticeTenantAdmin</c> (via
    /// <c>AddLatticeTenantAdminApi</c>) in the same service provider before this call.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is <c>null</c>.</exception>
    public static IEndpointRouteBuilder MapLatticeTenantAdminApiGrpc(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates the static
        // holder before Grpc.AspNetCore reflects [BindServiceMethod] and invokes the
        // static BindService callback at startup. MapGrpcService targets the
        // abstract base because it bears the attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeTenantAdminGrpcMethods>();
        endpoints.MapGrpcService<LatticeTenantAdminGrpcServiceBase>();

        return endpoints;
    }

    private static void RegisterMethodFactory(IServiceCollection services)
    {
        services.TryAddSingleton<LatticeTenantAdminGrpcMethods>(sp =>
        {
            var methods = LatticeTenantAdminGrpcMethods.FromServiceProvider(sp);
            LatticeTenantAdminGrpcMethodsHolder.Current = methods;
            return methods;
        });
    }
}
