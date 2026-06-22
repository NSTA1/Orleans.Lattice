using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// Reads the launcher-friendly bootstrap environment variables and exposes them
/// as a first-run configuration seed (<see cref="IExplorerConfigurationSeed"/>)
/// and an optional sign-in credential seed (<see cref="IExplorerCredentialSeed"/>).
/// </summary>
/// <remarks>
/// <para>The recognised variables are:</para>
/// <list type="bullet">
/// <item><description>
/// <see cref="EndpointVariable"/> (<c>LATTICE_EXPLORER_ENDPOINT</c>) - the
/// state-API endpoint URL. When unset, <see cref="TrySeed"/> returns
/// <see langword="null"/> and the explorer falls back to its normal first-run
/// (unconfigured) flow.
/// </description></item>
/// <item><description>
/// <see cref="InsecureDevVariable"/> (<c>LATTICE_EXPLORER_INSECURE_DEV</c>) -
/// when truthy, the seeded endpoint uses
/// <see cref="ExplorerTransportMode.InsecureLoopbackDev"/> with unencrypted
/// HTTP/2 allowed, for a local h2c development cluster. Otherwise the seed is
/// secure-by-default.
/// </description></item>
/// <item><description>
/// <see cref="UsernameVariable"/> (<c>LATTICE_EXPLORER_USERNAME</c>) and
/// <see cref="PasswordVariable"/> (<c>LATTICE_EXPLORER_PASSWORD</c>) - an
/// optional sign-in credential applied in memory for the current process.
/// </description></item>
/// </list>
/// <para>
/// The configuration seed never carries the credential: the username/password
/// flow is exposed strictly through the separate
/// <see cref="IExplorerCredentialSeed"/> surface, so a seeded endpoint is never
/// persisted with an embedded secret.
/// </para>
/// </remarks>
public sealed class EnvironmentExplorerBootstrap : IExplorerConfigurationSeed, IExplorerCredentialSeed
{
    /// <summary>The endpoint-seed variable name.</summary>
    public const string EndpointVariable = "LATTICE_EXPLORER_ENDPOINT";

    /// <summary>The insecure-loopback-dev opt-in variable name.</summary>
    public const string InsecureDevVariable = "LATTICE_EXPLORER_INSECURE_DEV";

    /// <summary>The optional config-file-path override variable name.</summary>
    public const string ConfigPathVariable = "LATTICE_EXPLORER_CONFIG";

    /// <summary>The optional sign-in username variable name.</summary>
    public const string UsernameVariable = "LATTICE_EXPLORER_USERNAME";

    /// <summary>The optional sign-in password variable name.</summary>
    public const string PasswordVariable = "LATTICE_EXPLORER_PASSWORD";

    private readonly IExplorerEnvironment _environment;

    /// <summary>Creates the bootstrap over the supplied environment seam.</summary>
    /// <param name="environment">The environment-variable reader.</param>
    public EnvironmentExplorerBootstrap(IExplorerEnvironment environment)
    {
        ArgumentNullException.ThrowIfNull(environment);
        _environment = environment;
    }

    /// <inheritdoc />
    ExplorerConfiguration? IExplorerConfigurationSeed.TrySeed()
    {
        var endpoint = _environment.GetVariable(EndpointVariable);
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return null;
        }

        var insecureDev = IsTruthy(_environment.GetVariable(InsecureDevVariable));

        return new ExplorerConfiguration
        {
            Endpoint = endpoint.Trim(),
            TransportMode = insecureDev
                ? ExplorerTransportMode.InsecureLoopbackDev
                : ExplorerTransportMode.Secure,
            AllowUnencryptedHttp2 = insecureDev,
            // Never seed a credential or metadata header here: the persisted
            // config store must never hold a secret. Sign-in is seeded
            // separately through IExplorerCredentialSeed.
            Headers = null,
        };
    }

    /// <inheritdoc />
    StoredCredential? IExplorerCredentialSeed.TrySeed()
    {
        var username = _environment.GetVariable(UsernameVariable);
        var password = _environment.GetVariable(PasswordVariable);
        if (string.IsNullOrWhiteSpace(username) || string.IsNullOrEmpty(password))
        {
            return null;
        }

        return new StoredCredential(username.Trim(), password);
    }

    private static bool IsTruthy(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() switch
        {
            "1" or "true" or "yes" or "on" => true,
            _ => false,
        };
    }
}
