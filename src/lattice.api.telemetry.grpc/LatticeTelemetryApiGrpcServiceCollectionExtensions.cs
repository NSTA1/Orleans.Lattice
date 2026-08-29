using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// DI extensions for wiring up the <c>Orleans.Lattice.Api.Telemetry.Grpc</c>
/// binding - the routable per-region gRPC surface over the telemetry facade.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is two calls. In the host's service composition:</para>
/// <code>
/// builder.Services.AddLatticeTelemetryApiGrpc(o => o.RequireAuthorization = true);
/// builder.Services.AddSingleton&lt;ILatticeTelemetryApiAuthorizer, MyTokenAuthorizer&gt;();
/// </code>
/// <para>And, in the ASP.NET Core endpoint composition:</para>
/// <code>
/// app.MapLatticeTelemetryApiGrpc();
/// </code>
/// <para>
/// The host must also expose the facade
/// (<c>Orleans.Lattice.Api.Telemetry.ILatticeTelemetry</c>) in the same service
/// provider. That is a single call - <c>services.AddLatticeTelemetryApi()</c> -
/// which wires the backend client itself and is idempotent, so this binding
/// neither repeats nor re-configures it. That registration resolves the access
/// gate, membership context, and tenant-context resolver <em>optionally</em>, so a
/// minimal host still gets a working, fail-closed facade pinned to the default
/// tenant; this binding likewise assumes none of them are present and depends only
/// on <c>ILatticeTelemetry</c>.
/// </para>
/// <para>
/// The binding fails closed: with the default
/// <see cref="DenyTelemetryApiAuthorizer"/> and
/// <see cref="LatticeTelemetryApiGrpcOptions.RequireAuthorization"/> at its
/// <see langword="true"/> default, every call is rejected until the host opts in
/// via a permissive authorizer or by turning enforcement off.
/// </para>
/// </remarks>
public static class LatticeTelemetryApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <c>Orleans.Lattice.Api.Telemetry.Grpc</c> binding: the
    /// method-definition singleton, the server-side service, the default-deny
    /// authorizer, the credential bridge, the auth-scheme source, and the
    /// authorization interceptor. Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeTelemetryApiGrpcOptions"/>.
    /// </param>
    /// <returns>The same service collection for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddLatticeTelemetryApiGrpc(
        this IServiceCollection services,
        Action<LatticeTelemetryApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeTelemetryApiGrpcOptions>();
        }

        RegisterMethodFactory(services);

        // Default-deny authorizer. TryAdd preserves any permissive authorizer the
        // host registered first; the fallback keeps the surface closed.
        services.TryAddSingleton<ILatticeTelemetryApiAuthorizer, DenyTelemetryApiAuthorizer>();

        // Identity bridge: lifts the inbound gRPC credential header into the ambient
        // Lattice credential so the facade can resolve the caller's subject and
        // derive the effective tenant. TryAdd preserves a host-supplied bridge (for
        // a bespoke identity source such as a client certificate).
        services.TryAddSingleton<ILatticeTelemetryApiCredentialBridge, HeaderLatticeTelemetryApiCredentialBridge>();

        // Auth-scheme advertisement source: the unauthenticated GetAuthScheme RPC
        // reads the schemes a host configured via options. TryAdd preserves a
        // host-supplied source. Advertises nothing by default.
        services.TryAddSingleton<ILatticeTelemetryApiAuthSchemeSource, OptionsLatticeTelemetryApiAuthSchemeSource>();

        // Register the auth interceptor globally; it scopes enforcement to the
        // telemetry service by service-name prefix so unrelated gRPC services on the
        // same host are unaffected.
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<LatticeTelemetryApiGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeTelemetryApiGrpcAuthInterceptor>();

        services.TryAddSingleton<LatticeTelemetryGrpcService>();
        services.TryAddSingleton<LatticeTelemetryGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeTelemetryGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps the telemetry RPC routes (catalogue discovery, query evaluation, and
    /// the unauthenticated auth-scheme discovery RPC) on the supplied
    /// <paramref name="endpoints"/>. The host must have called
    /// <see cref="AddLatticeTelemetryApiGrpc"/> and must expose
    /// <c>Orleans.Lattice.Api.Telemetry.ILatticeTelemetry</c> in the same service
    /// provider before this call.
    /// </summary>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <returns>The endpoint route builder for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="endpoints"/> is <see langword="null"/>.</exception>
    public static IEndpointRouteBuilder MapLatticeTelemetryApiGrpc(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates the static
        // holder before Grpc.AspNetCore reflects [BindServiceMethod] and invokes the
        // static BindService callback at startup. MapGrpcService targets the
        // abstract base because it bears the attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeTelemetryGrpcMethods>();
        endpoints.MapGrpcService<LatticeTelemetryGrpcServiceBase>();

        return endpoints;
    }

    private static void RegisterMethodFactory(IServiceCollection services)
    {
        services.TryAddSingleton<LatticeTelemetryGrpcMethods>(sp =>
        {
            var methods = LatticeTelemetryGrpcMethods.FromServiceProvider(sp);
            LatticeTelemetryGrpcMethodsHolder.Current = methods;
            return methods;
        });
    }
}
