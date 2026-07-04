using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Auth.Grpc;

/// <summary>
/// DI extensions for wiring up the <c>Orleans.Lattice.Api.Auth.Grpc</c>
/// binding - the gRPC surface over the membership and policy admin facade.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is two calls. In the host's service composition:</para>
/// <code>
/// builder.Services.AddLatticeAuthApiGrpc(o => o.RequireAuthorization = true);
/// builder.Services.AddSingleton&lt;ILatticeAuthApiAuthorizer, MyTokenAuthorizer&gt;();
/// </code>
/// <para>And, in the ASP.NET Core endpoint composition:</para>
/// <code>
/// app.MapLatticeAuthApiGrpc();
/// </code>
/// <para>
/// The host must also expose the facade (<c>ILatticeAuthAdmin</c>) in the same
/// service provider - typically by co-hosting Orleans with
/// <c>AddLattice(...).AddLatticeAuth(...).AddLatticeAuthApi()</c> on the same
/// host. The binding fails closed: with the default
/// <see cref="DenyAllAuthApiAuthorizer"/> and
/// <see cref="LatticeAuthApiGrpcOptions.RequireAuthorization"/> at its
/// <see langword="true"/> default, every call is rejected until the host opts in
/// via a permissive authorizer or by turning enforcement off. Even then, the
/// facade's own administrator check still runs against the resolved caller's
/// subject, so turning transport enforcement off never opens the surface to
/// anonymous or non-administrator callers.
/// </para>
/// </remarks>
public static class LatticeAuthApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <c>Orleans.Lattice.Api.Auth.Grpc</c> binding: the
    /// method-definition singleton, the server-side service, the default-deny
    /// meta-authorizer, the identity bridge, and the authorization interceptor.
    /// Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeAuthApiGrpcOptions"/>.
    /// </param>
    public static IServiceCollection AddLatticeAuthApiGrpc(
        this IServiceCollection services,
        Action<LatticeAuthApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeAuthApiGrpcOptions>();
        }

        RegisterMethodFactory(services);

        // Default-deny meta-authorizer. TryAdd preserves any permissive
        // authorizer the host registered first; the fallback keeps the surface
        // closed.
        services.TryAddSingleton<ILatticeAuthApiAuthorizer, DenyAllAuthApiAuthorizer>();

        // Identity bridge: lifts the inbound gRPC credential header into the
        // ambient Lattice credential so the facade's administrator check can
        // resolve the caller's subject. TryAdd preserves a host-supplied bridge
        // (for a bespoke identity source such as a client certificate).
        services.TryAddSingleton<ILatticeAuthApiCredentialBridge, HeaderLatticeAuthApiCredentialBridge>();

        // Register the auth interceptor globally; it scopes enforcement to the
        // auth-API service by service-name prefix so unrelated gRPC services on
        // the same host are unaffected.
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<LatticeAuthApiGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeAuthApiGrpcAuthInterceptor>();

        services.TryAddSingleton<LatticeAuthApiGrpcService>();
        services.TryAddSingleton<LatticeAuthApiGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeAuthApiGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps the auth-API RPC routes (the membership, policy, and introspection
    /// unary RPCs) on the supplied <paramref name="endpoints"/>. The host must
    /// have called <see cref="AddLatticeAuthApiGrpc"/> and must expose
    /// <c>ILatticeAuthAdmin</c> (via <c>AddLatticeAuthApi</c>) in the same
    /// service provider before this call.
    /// </summary>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapLatticeAuthApiGrpc(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates the static
        // holder before Grpc.AspNetCore reflects [BindServiceMethod] and invokes
        // the static BindService callback at startup. MapGrpcService targets the
        // abstract base because it bears the attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeAuthApiGrpcMethods>();
        endpoints.MapGrpcService<LatticeAuthApiGrpcServiceBase>();

        return endpoints;
    }

    private static void RegisterMethodFactory(IServiceCollection services)
    {
        services.TryAddSingleton<LatticeAuthApiGrpcMethods>(sp =>
        {
            var methods = LatticeAuthApiGrpcMethods.FromServiceProvider(sp);
            LatticeAuthApiGrpcMethodsHolder.Current = methods;
            return methods;
        });
    }
}
