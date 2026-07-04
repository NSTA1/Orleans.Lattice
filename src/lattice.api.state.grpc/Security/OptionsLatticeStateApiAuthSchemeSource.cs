using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State.Grpc;

/// <summary>
/// The default <see cref="ILatticeStateApiAuthSchemeSource"/>: builds the
/// advertisement from <see cref="LatticeStateApiGrpcOptions.AdvertisedAuthSchemes"/>.
/// A host configures the schemes through options; with none configured (the
/// default) the advertisement is empty and clients fall back to manual or Basic
/// selection.
/// </summary>
internal sealed class OptionsLatticeStateApiAuthSchemeSource : ILatticeStateApiAuthSchemeSource
{
    private readonly IOptionsMonitor<LatticeStateApiGrpcOptions> _options;

    /// <summary>Initialises the source over the binding options.</summary>
    public OptionsLatticeStateApiAuthSchemeSource(IOptionsMonitor<LatticeStateApiGrpcOptions> options)
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
