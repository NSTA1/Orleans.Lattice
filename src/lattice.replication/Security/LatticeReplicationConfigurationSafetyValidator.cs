using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Startup-time scan that fails closed when a secret-shaped
/// configuration key is sourced from a file under the application
/// directory. Catches the most common accidental-commit pathway:
/// <c>appsettings.json</c> being checked into source control with a
/// populated <c>LatticeReplication:Secret</c> entry. Operators that
/// intentionally source secrets via a file (typically the .NET
/// user-secrets store, which lives outside the app directory) or via
/// any non-file provider (env vars, key vault, in-memory) are
/// unaffected.
/// </summary>
/// <remarks>
/// The validator is registered as an <see cref="IHostedService"/> with
/// a startup-only contract: it runs once on <see cref="StartAsync"/>,
/// throws if the scan trips, and is otherwise a no-op. Running it
/// before the rest of the silo lights up means an unsafe configuration
/// fails the host before any secret has been read and before any port
/// is open.
/// </remarks>
internal sealed class LatticeReplicationConfigurationSafetyValidator(
    IServiceProvider services,
    IOptionsMonitor<LatticeReplicationSecurityOptions> options,
    ILogger<LatticeReplicationConfigurationSafetyValidator> logger) : IHostedService
{
    /// <summary>The flat configuration key for the cluster-wide secret.</summary>
    internal const string SecretConfigKey = "LatticeReplication:Secret";

    /// <summary>The flat configuration key for the accepted-set.</summary>
    internal const string AcceptedSecretsConfigKey = "LatticeReplication:AcceptedSecrets";

    /// <summary>The nested configuration key for the cluster-wide secret as bound by <see cref="ConfigurationBindingSecretSource"/>.</summary>
    internal const string NestedSecretConfigKey = "LatticeReplication:Secrets:Secret";

    /// <summary>The nested configuration section for the accepted-set as bound by <see cref="ConfigurationBindingSecretSource"/>.</summary>
    internal const string NestedAcceptedSecretsConfigKey = "LatticeReplication:Secrets:AcceptedSecrets";

    /// <summary>The configuration section conventionally used to bind the per-peer secret table.</summary>
    internal const string PeerSecretsConfigKeyPrefix = "LatticeReplication:Secrets:PeerSecrets";

    private readonly IServiceProvider _services = services ?? throw new ArgumentNullException(nameof(services));
    private readonly IOptionsMonitor<LatticeReplicationSecurityOptions> _options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<LatticeReplicationConfigurationSafetyValidator> _logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.CurrentValue.ScanConfigurationForSecrets)
        {
            _logger.LogWarning(
                "Lattice.Replication: configuration-secret scan is disabled by LatticeReplicationSecurityOptions.ScanConfigurationForSecrets=false. Secrets sourced from appsettings.json will not be detected at startup.");
            return Task.CompletedTask;
        }

        var allowEnv = Environment.GetEnvironmentVariable(
            LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets);
        if (IsTruthy(allowEnv))
        {
            _logger.LogWarning(
                "Lattice.Replication: configuration-secret scan is disabled by environment variable {Variable}. Secrets sourced from appsettings.json will not be detected at startup.",
                LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets);
            return Task.CompletedTask;
        }

        // Resolve IConfiguration optionally - test hosts that wire only
        // AddLatticeReplication against a bare ServiceCollection (no
        // Host/HostBuilder) won't have one registered, and the scan is
        // a no-op in that scenario.
        var configuration = _services.GetService<IConfiguration>();
        if (configuration is not IConfigurationRoot root)
        {
            return Task.CompletedTask;
        }

        var appDirectory = AppContext.BaseDirectory;
        var leakingKeys = new List<string>(capacity: 2);
        foreach (var key in EnumerateSecretShapedKeys(configuration))
        {
            if (TryFindFileBackedProvider(root, key, appDirectory, out var providerDescription))
            {
                leakingKeys.Add($"{key} (sourced from {providerDescription})");
            }
        }

        if (leakingKeys.Count > 0)
        {
            var msg = "Lattice.Replication refuses to start because shared-secret material is being supplied via a file-backed configuration provider under the application directory. "
                + "Files under the application directory have a high accidental-commit rate; secrets must be supplied via environment variables (LATTICE_REPLICATION_SECRET / LATTICE_REPLICATION_ACCEPTED_SECRETS), via the .NET user-secrets store, or via a custom ILatticeReplicationSecretSource. "
                + "Offending keys: "
                + string.Join("; ", leakingKeys)
                + ". To override (e.g. for a sealed-deployment artefact that ships pre-baked secrets), set "
                + LatticeReplicationEnvironmentVariables.AllowSourceTreeSecrets
                + "=1 or LatticeReplicationSecurityOptions.ScanConfigurationForSecrets=false.";
            throw new InvalidOperationException(msg);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static IEnumerable<string> EnumerateSecretShapedKeys(IConfiguration configuration)
    {
        // Two binding shapes are documented: the flat shape
        // (LatticeReplication:Secret + LatticeReplication:AcceptedSecrets,
        // matching the env-var spellings) and the nested shape used by
        // ConfigurationBindingSecretSource (LatticeReplication:Secrets:Secret,
        // LatticeReplication:Secrets:AcceptedSecrets, LatticeReplication:Secrets:PeerSecrets).
        // Both must be scanned, otherwise an operator who follows the
        // ConfigurationBindingSecretSource xml-doc example would have
        // their primary and accepted secrets pass the scan undetected.
        if (!string.IsNullOrEmpty(configuration[SecretConfigKey]))
        {
            yield return SecretConfigKey;
        }

        if (!string.IsNullOrEmpty(configuration[NestedSecretConfigKey]))
        {
            yield return NestedSecretConfigKey;
        }

        // Accepted-set may be bound as either a string ("alpha,beta") or
        // a JSON array ([ "alpha", "beta" ]). The array path materialises
        // as child keys (":0", ":1", ...), so yield each populated child
        // key directly - TryGet on the bare section name returns null
        // when the section is array-shaped.
        if (!string.IsNullOrEmpty(configuration[AcceptedSecretsConfigKey]))
        {
            yield return AcceptedSecretsConfigKey;
        }
        foreach (var child in configuration.GetSection(AcceptedSecretsConfigKey).GetChildren())
        {
            if (!string.IsNullOrEmpty(child.Value))
            {
                yield return child.Path;
            }
        }

        if (!string.IsNullOrEmpty(configuration[NestedAcceptedSecretsConfigKey]))
        {
            yield return NestedAcceptedSecretsConfigKey;
        }
        foreach (var child in configuration.GetSection(NestedAcceptedSecretsConfigKey).GetChildren())
        {
            if (!string.IsNullOrEmpty(child.Value))
            {
                yield return child.Path;
            }
        }

        foreach (var child in configuration.GetSection(PeerSecretsConfigKeyPrefix).GetChildren())
        {
            if (!string.IsNullOrEmpty(child.Value))
            {
                yield return child.Path;
            }
        }
    }

    /// <summary>
    /// Walks the <see cref="IConfigurationRoot.Providers"/> chain and
    /// returns <see langword="true"/> when the named key is served by a
    /// file-based provider whose underlying path lies inside the
    /// application directory. The .NET user-secrets store also uses a
    /// JSON file provider, but its file lives under
    /// <c>%APPDATA%/Microsoft/UserSecrets</c> (or the platform
    /// equivalent), which is outside the application directory and
    /// thus does not trip the scan.
    /// </summary>
    private static bool TryFindFileBackedProvider(
        IConfigurationRoot root,
        string key,
        string appDirectory,
        out string providerDescription)
    {
        foreach (var provider in root.Providers)
        {
            if (provider.TryGet(key, out var value) && !string.IsNullOrEmpty(value))
            {
                if (TryGetFileProviderPath(provider, out var path))
                {
                    var fullPath = Path.GetFullPath(path);
                    var fullAppDir = Path.GetFullPath(appDirectory);
                    if (fullPath.StartsWith(fullAppDir, StringComparison.OrdinalIgnoreCase))
                    {
                        providerDescription = $"{provider.GetType().Name} -> {fullPath}";
                        return true;
                    }
                }
            }
        }

        providerDescription = string.Empty;
        return false;
    }

    private static bool TryGetFileProviderPath(IConfigurationProvider provider, out string path)
    {
        // FileConfigurationProvider exposes Source.Path via a public Source
        // property; reflect rather than take a hard dependency on the
        // file-extension type (which would force a transitive package
        // reference on every consumer).
        var sourceProp = provider.GetType().GetProperty("Source");
        if (sourceProp?.GetValue(provider) is { } source)
        {
            var pathProp = source.GetType().GetProperty("Path");
            if (pathProp?.GetValue(source) is string p && !string.IsNullOrEmpty(p))
            {
                path = p;
                return true;
            }
        }

        path = string.Empty;
        return false;
    }

    private static bool IsTruthy(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim() switch
        {
            "1" => true,
            var s when s.Equals("true", StringComparison.OrdinalIgnoreCase) => true,
            var s when s.Equals("yes", StringComparison.OrdinalIgnoreCase) => true,
            _ => false,
        };
    }
}
