using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// DI extensions for wiring up the <c>Orleans.Lattice.Api.Backup.Grpc</c>
/// binding - the gRPC surface over the backup control facade.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is two calls. In the host's service composition:</para>
/// <code>
/// builder.Services.AddLatticeBackupApiGrpc(o => o.RequireAuthorization = true);
/// builder.Services.AddSingleton&lt;ILatticeBackupApiAuthorizer, MyTokenAuthorizer&gt;();
/// </code>
/// <para>And, in the ASP.NET Core endpoint composition:</para>
/// <code>
/// app.MapLatticeBackupApiGrpc();
/// </code>
/// <para>
/// The host must also expose the facade
/// (<c>Orleans.Lattice.Api.Backup.ILatticeBackupControl</c>) in the same service
/// provider - typically by co-hosting Orleans with
/// <c>AddLattice(...).AddLatticeBackup(...).AddLatticeBackupApi()</c> on the same
/// host. The binding fails closed: with the default
/// <see cref="DenyAllBackupApiAuthorizer"/> and
/// <see cref="LatticeBackupApiGrpcOptions.RequireAuthorization"/> at its
/// <see langword="true"/> default, every call is rejected until the host opts in
/// via a permissive authorizer or by turning enforcement off.
/// </para>
/// </remarks>
public static class LatticeBackupApiGrpcServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <c>Orleans.Lattice.Api.Backup.Grpc</c> binding: the
    /// method-definition singleton, the server-side service, the default-deny
    /// authorizer, and the authorization interceptor. Idempotent.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate that populates <see cref="LatticeBackupApiGrpcOptions"/>.
    /// </param>
    public static IServiceCollection AddLatticeBackupApiGrpc(
        this IServiceCollection services,
        Action<LatticeBackupApiGrpcOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<LatticeBackupApiGrpcOptions>();
        }

        RegisterMethodFactory(services);

        // Default-deny authorizer. TryAdd preserves any permissive authorizer
        // the host registered first; the fallback keeps the surface closed.
        services.TryAddSingleton<ILatticeBackupApiAuthorizer, DenyAllBackupApiAuthorizer>();

        // Identity bridge: lifts the inbound gRPC credential header into the
        // ambient Lattice credential so the backup access gate can resolve the
        // caller's subject. TryAdd preserves a host-supplied bridge (for a
        // bespoke identity source such as a client certificate).
        services.TryAddSingleton<ILatticeBackupApiCredentialBridge, HeaderLatticeBackupApiCredentialBridge>();

        // Auth-scheme advertisement source: the unauthenticated GetAuthScheme RPC
        // reads the schemes a host configured via options. TryAdd preserves a
        // host-supplied source. Advertises nothing by default.
        services.TryAddSingleton<ILatticeBackupApiAuthSchemeSource, OptionsLatticeBackupApiAuthSchemeSource>();

        // Register the auth interceptor globally; it scopes enforcement to the
        // backup control-API service by service-name prefix so unrelated gRPC
        // services on the same host are unaffected.
        services.AddGrpc(options =>
        {
            options.Interceptors.Add<LatticeBackupApiGrpcAuthInterceptor>();
        });
        services.TryAddSingleton<LatticeBackupApiGrpcAuthInterceptor>();

        services.TryAddSingleton<LatticeBackupGrpcService>();
        services.TryAddSingleton<LatticeBackupGrpcServiceBase>(
            sp => sp.GetRequiredService<LatticeBackupGrpcService>());

        return services;
    }

    /// <summary>
    /// Maps the backup control-API RPC routes (the unary capture / list /
    /// describe / delete / restore / revert / auth-scheme RPCs plus the
    /// server-streaming <c>StreamBackups</c> and <c>ExportArtifact</c> RPCs) on
    /// the supplied <paramref name="endpoints"/>. The host must have called
    /// <see cref="AddLatticeBackupApiGrpc"/> and must expose
    /// <c>Orleans.Lattice.Api.Backup.ILatticeBackupControl</c> (via
    /// <c>AddLatticeBackupApi</c>) in the same service provider before this call.
    /// </summary>
    /// <returns>The endpoint route builder for chaining.</returns>
    public static IEndpointRouteBuilder MapLatticeBackupApiGrpc(
        this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        // Pre-resolve the method singleton so its factory populates the static
        // holder before Grpc.AspNetCore reflects [BindServiceMethod] and
        // invokes the static BindService callback at startup. MapGrpcService
        // targets the abstract base because it bears the attribute.
        endpoints.ServiceProvider.GetRequiredService<LatticeBackupGrpcMethods>();
        endpoints.MapGrpcService<LatticeBackupGrpcServiceBase>();

        return endpoints;
    }

    private static void RegisterMethodFactory(IServiceCollection services)
    {
        services.TryAddSingleton<LatticeBackupGrpcMethods>(sp =>
        {
            var methods = LatticeBackupGrpcMethods.FromServiceProvider(sp);
            LatticeBackupGrpcMethodsHolder.Current = methods;
            return methods;
        });
    }
}
