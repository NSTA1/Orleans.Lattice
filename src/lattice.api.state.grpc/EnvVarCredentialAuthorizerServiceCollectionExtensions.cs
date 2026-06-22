using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// DI extensions for registering the reference
/// <see cref="EnvVarCredentialAuthorizer"/> on a host that exposes the state-API
/// gRPC surface, turning a default-deny endpoint into a turnkey secured one
/// validated against an environment-variable credential dictionary.
/// </summary>
/// <remarks>
/// <para>The canonical wiring is:</para>
/// <code>
/// builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = true);
/// builder.Services.AddEnvVarCredentialAuthorizer();
/// </code>
/// <para>
/// with each operator credential supplied as an environment variable
/// (<c>LATTICE_STATE_USER_alice=pbkdf2-sha256$...</c>) produced by the
/// credential-generation helper scripts under <c>tools/</c>.
/// </para>
/// </remarks>
public static class EnvVarCredentialAuthorizerServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="EnvVarCredentialAuthorizer"/> as the active
    /// <see cref="ILatticeStateApiAuthorizer"/>, replacing the default-deny
    /// authorizer, and the supporting <see cref="IEnvironmentVariableReader"/>.
    /// </summary>
    /// <param name="services">The host's service collection.</param>
    /// <param name="configure">
    /// Optional delegate to populate <see cref="EnvVarCredentialAuthorizerOptions"/>
    /// (the env-var prefix and the failed-attempt lockout policy).
    /// </param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddEnvVarCredentialAuthorizer(
        this IServiceCollection services,
        Action<EnvVarCredentialAuthorizerOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configure is not null)
        {
            services.Configure(configure);
        }
        else
        {
            services.AddOptions<EnvVarCredentialAuthorizerOptions>();
        }

        services.TryAddSingleton<IEnvironmentVariableReader, ProcessEnvironmentVariableReader>();

        // Replace the default-deny (or any previously registered) authorizer so
        // the env-var credential validator becomes the active policy.
        services.RemoveAll<ILatticeStateApiAuthorizer>();
        services.AddSingleton<ILatticeStateApiAuthorizer, EnvVarCredentialAuthorizer>();

        return services;
    }
}
