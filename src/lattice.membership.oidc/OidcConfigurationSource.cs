using System.Collections.Concurrent;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Orleans.Lattice.Membership.Oidc;

/// <summary>
/// The production <see cref="IOidcConfigurationSource"/>. It builds one
/// <see cref="ConfigurationManager{OpenIdConnectConfiguration}"/> per metadata
/// address and caches it, so the OpenID Connect discovery document and its JWKS
/// signing keys are fetched once, then refreshed on the configuration manager's
/// own schedule (automatic-refresh and minimum-refresh intervals) with
/// last-known-good fallback. No fetch happens on the per-authentication path.
/// </summary>
internal sealed class OidcConfigurationSource : IOidcConfigurationSource
{
    private readonly ConcurrentDictionary<string, BaseConfigurationManager> _cache =
        new(StringComparer.Ordinal);
    private readonly Func<string, BaseConfigurationManager> _factory;
    private readonly TimeSpan _automaticRefreshInterval;
    private readonly TimeSpan _refreshInterval;

    /// <summary>
    /// Initializes a new <see cref="OidcConfigurationSource"/>.
    /// </summary>
    /// <param name="automaticRefreshInterval">How often the discovered metadata is proactively refreshed.</param>
    /// <param name="refreshInterval">The minimum interval between forced refreshes.</param>
    public OidcConfigurationSource(TimeSpan automaticRefreshInterval, TimeSpan refreshInterval)
    {
        _automaticRefreshInterval = automaticRefreshInterval;
        _refreshInterval = refreshInterval;

        // Cached once. A method-group conversion of the instance method Create
        // captures `this`, so passing it inline to GetOrAdd would allocate a
        // delegate on every authentication (GetOrCreate is on the per-credential
        // validation path).
        _factory = Create;
    }

    /// <inheritdoc />
    public BaseConfigurationManager GetOrCreate(string metadataAddress)
    {
        ArgumentException.ThrowIfNullOrEmpty(metadataAddress);
        return _cache.GetOrAdd(metadataAddress, _factory);
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
