using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// DI extensions for wiring up the <c>Orleans.Lattice.Api.Replication.Grpc</c>
/// binding - the gRPC surface over the replication control facade.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is two calls. In the host's service composition:</para>
/// <code>
/// builder.Services.AddLatticeReplicationApiGrpc(o => o.RequireAuthorization = true);
/// builder.Services.AddSingleton&lt;ILatticeReplicationApiAuthorizer, MyTokenAuthorizer&gt;();
/// </code>
/// <para>And, in the ASP.NET Core endpoint composition:</para>
/// <code>
/// app.MapLatticeReplicationApiGrpc();
/// </code>
/// <para>
/// The host must also expose the facade
/// (<c>Orleans.Lattice.Api.Replication.ILatticeReplicationControl</c>) in the
/// same service provider - typically by co-hosting Orleans with
/// <c>AddLattice(...)</c> and <c>AddLatticeReplicationApi()</c> on the same host.
/// The binding fails closed: with the default
/// <see cref="DenyAllReplicationApiAuthorizer"/> and
/// <see cref="LatticeReplicationApiGrpcOptions.RequireAuthorization"/> at its
/// <see langword="true"/> default, every call is rejected until the host opts in
/// via a permissive authorizer or by turning enforcement off.
/// </para>
/// </remarks>
public static class LatticeReplicationApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <c>Orleans.Lattice.Api.Replication.Grpc</c> binding: the
    /// method-definition singleton, the server-side service, the default-deny
    /// authorizer, the credential bridge, the auth-scheme source, and the
    /// authorization interceptor. Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates
    /// <see cref="LatticeReplicationApiGrpcOptions"/>.
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddLatticeReplicationApiGrpc(
        this IServiceCollection services,
        Action<LatticeReplicationApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeReplicationApiGrpcOptions>();
        }

        RegisterMethodFactory(services);

        // Default-deny authorizer. TryAdd preserves any permissive authorizer
        // the host registered first; the fallback keeps the surface closed.
        services.TryAddSingleton<ILatticeReplicationApiAuthorizer, DenyAllReplicationApiAuthorizer>();

        // Identity bridge: lifts the inbound gRPC credential header into the
        // ambient Lattice credential so the replication access gate can resolve
        // the caller's subject. TryAdd preserves a host-supplied bridge (for a
        // bespoke identity source such as a client certificate).
        services.TryAddSingleton<ILatticeReplicationApiCredentialBridge, HeaderLatticeReplicationApiCredentialBridge>();

        // Auth-scheme advertisement source: the unauthenticated GetAuthScheme RPC
        // reads the schemes a host configured via options. TryAdd preserves a
        // host-supplied source. Advertises nothing by default.
        services.TryAddSingleton<ILatticeReplicationApiAuthSchemeSource, OptionsLatticeReplicationApiAuthSchemeSource>();

        // Register the auth interceptor globally; it scopes enforcement to the
        // replication control-API service by service-name prefix so unrelated
        // gRPC services on the same host are unaffected.
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<LatticeReplicationApiGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeReplicationApiGrpcAuthInterceptor>();

        services.TryAddSingleton<LatticeReplicationGrpcService>();
        services.TryAddSingleton<LatticeReplicationGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeReplicationGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps the replication control-API RPC routes (the unary
    /// <c>EnableReplication</c>, <c>DisableReplication</c>,
    /// <c>GetReplicationConfig</c>, and unauthenticated <c>GetAuthScheme</c> RPCs)
    /// on the supplied <paramref name="endpoints"/>. The host must have called
    /// <see cref="AddLatticeReplicationApiGrpc"/> and must expose
    /// <c>Orleans.Lattice.Api.Replication.ILatticeReplicationControl</c> (via
    /// <c>AddLatticeReplicationApi</c>) in the same service provider before this
    /// call.
    /// </summary>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapLatticeReplicationApiGrpc(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates the static
        // holder before Grpc.AspNetCore reflects [BindServiceMethod] and
        // invokes the static BindService callback at startup. MapGrpcService
        // targets the abstract base because it bears the attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeReplicationGrpcMethods>();
        endpoints.MapGrpcService<LatticeReplicationGrpcServiceBase>();

        return endpoints;
    }

    private static void RegisterMethodFactory(IServiceCollection services)
    {
        services.TryAddSingleton<LatticeReplicationGrpcMethods>(sp =>
        {
            var methods = LatticeReplicationGrpcMethods.FromServiceProvider(sp);
            LatticeReplicationGrpcMethodsHolder.Current = methods;
            return methods;
        });
    }
}
