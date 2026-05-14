using Microsoft.Extensions.Configuration;

namespace Orleans.Lattice.Replication;

/// <summary>
/// <see cref="ILatticeReplicationSecretSource"/> that reads secrets from
/// an <see cref="IConfiguration"/> section. Intended for hosts that
/// inject secrets via a non-file configuration provider - typically the
/// .NET user-secrets store in development, an Azure App Configuration
/// or AWS Parameter Store provider in production, or an in-process
/// <c>MemoryConfigurationProvider</c> seeded from a key vault SDK at
/// startup. <b>Not</b> intended to bind <c>appsettings.json</c>; the
/// hostile-config scan (enabled by default via
/// <see cref="LatticeReplicationSecurityOptions.ScanConfigurationForSecrets"/>)
/// rejects file-backed bindings at startup precisely because checked-in
/// <c>appsettings.json</c> secrets are the dominant accidental-commit
/// vector.
/// </summary>
/// <remarks>
/// Expected schema (under whatever section the host binds):
/// <code>
/// {
///   "Secret": "...",
///   "AcceptedSecrets": [ "...", "..." ],
///   "PeerSecrets": {
///     "us-west-2": "..."
///   }
/// }
/// </code>
/// </remarks>
public sealed class ConfigurationBindingSecretSource : ILatticeReplicationSecretSource
{
    private readonly IConfiguration _section;

    /// <summary>
    /// Creates a secret source that reads from the supplied
    /// configuration section.
    /// </summary>
    /// <param name="section">
    /// The configuration section that contains the
    /// <c>Secret</c>, <c>AcceptedSecrets</c>, and
    /// <c>PeerSecrets</c> keys. Typically obtained via
    /// <c>configuration.GetSection("LatticeReplication:Secrets")</c>.
    /// </param>
    public ConfigurationBindingSecretSource(IConfiguration section)
    {
        ArgumentNullException.ThrowIfNull(section);
        _section = section;
    }

    /// <inheritdoc />
    public ValueTask<string?> GetOutboundSecretAsync(string peerClusterId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(peerClusterId);

        var perPeer = _section.GetSection("PeerSecrets")[peerClusterId];
        if (!string.IsNullOrWhiteSpace(perPeer))
        {
            return new ValueTask<string?>(perPeer);
        }

        var clusterWide = _section["Secret"];
        return new ValueTask<string?>(string.IsNullOrWhiteSpace(clusterWide) ? null : clusterWide);
    }

    /// <inheritdoc />
    public ValueTask<LatticeReplicationAcceptedSecrets> GetAcceptedSecretsAsync(CancellationToken cancellationToken)
    {
        var combined = new List<string>(capacity: 4);
        var primary = _section["Secret"];
        if (!string.IsNullOrWhiteSpace(primary))
        {
            combined.Add(primary);
        }

        var acceptedSection = _section.GetSection("AcceptedSecrets");
        foreach (var child in acceptedSection.GetChildren())
        {
            var value = child.Value;
            if (!string.IsNullOrWhiteSpace(value) && !combined.Contains(value, StringComparer.Ordinal))
            {
                combined.Add(value);
            }
        }

        if (combined.Count == 0)
        {
            return new ValueTask<LatticeReplicationAcceptedSecrets>(LatticeReplicationAcceptedSecrets.Empty);
        }

        var version = StableSecretSetHash.Compute(combined);
        return new ValueTask<LatticeReplicationAcceptedSecrets>(
            new LatticeReplicationAcceptedSecrets(combined, version));
    }
}
