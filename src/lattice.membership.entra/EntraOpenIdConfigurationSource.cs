using System.Collections.Concurrent;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Entra;

/// <summary>
/// The production <see cref="IEntraOpenIdConfigurationSource"/>. It builds one
/// <see cref="ConfigurationManager{OpenIdConnectConfiguration}"/> per metadata
/// address and caches it, so OIDC metadata and the JWKS signing keys are
/// discovered once, then refreshed on the configuration manager's own schedule
/// (automatic-refresh and minimum-refresh intervals) with last-known-good
/// fallback. No fetch happens on the per-authentication path.
/// </summary>
internal sealed class EntraOpenIdConfigurationSource : IEntraOpenIdConfigurationSource
{
    private readonly ConcurrentDictionary<string, BaseConfigurationManager> _cache =
        new(StringComparer.Ordinal);
    private readonly TimeSpan _automaticRefreshInterval;
    private readonly TimeSpan _refreshInterval;

    /// <summary>
    /// Initializes a new <see cref="EntraOpenIdConfigurationSource"/>.
    /// </summary>
    /// <param name="automaticRefreshInterval">How often the JWKS metadata is proactively refreshed.</param>
    /// <param name="refreshInterval">The minimum interval between forced refreshes.</param>
    public EntraOpenIdConfigurationSource(TimeSpan automaticRefreshInterval, TimeSpan refreshInterval)
    {
        _automaticRefreshInterval = automaticRefreshInterval;
        _refreshInterval = refreshInterval;
    }

    /// <inheritdoc />
    public BaseConfigurationManager GetOrCreate(string metadataAddress)
    {
        ArgumentException.ThrowIfNullOrEmpty(metadataAddress);
        return _cache.GetOrAdd(metadataAddress, Create);
    }

    private BaseConfigurationManager Create(string metadataAddress)
    {
        var manager = new ConfigurationManager<OpenIdConnectConfiguration>(
            metadataAddress,
            new OpenIdConnectConfigurationRetriever())
        {
            AutomaticRefreshInterval = _automaticRefreshInterval,
            RefreshInterval = _refreshInterval,
        };

        return manager;
    }
}
