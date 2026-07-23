using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// The default <see cref="ILatticeReplicationApiAuthSchemeSource"/>: builds the
/// advertisement from
/// <see cref="LatticeReplicationApiGrpcOptions.AdvertisedAuthSchemes"/>. A host
/// configures the schemes through options; with none configured (the default)
/// the advertisement is empty and clients fall back to manual or Basic
/// selection.
/// </summary>
internal sealed class OptionsLatticeReplicationApiAuthSchemeSource : ILatticeReplicationApiAuthSchemeSource
{
    private readonly IOptionsMonitor<LatticeReplicationApiGrpcOptions> _options;

    /// <summary>Initialises the source over the binding options.</summary>
    public OptionsLatticeReplicationApiAuthSchemeSource(IOptionsMonitor<LatticeReplicationApiGrpcOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public AuthSchemeAdvertisement GetAdvertisement()
    {
        var configured = _options.CurrentValue.AdvertisedAuthSchemes;
        if (configured.Count == 0)
        {
            return new AuthSchemeAdvertisement();
        }

        return new AuthSchemeAdvertisement { Schemes = configured.ToArray() };
    }
}
