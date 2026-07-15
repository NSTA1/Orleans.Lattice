using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Schema.Grpc;

/// <summary>
/// The default <see cref="ILatticeSchemaApiAuthSchemeSource"/>: builds the
/// advertisement from <see cref="LatticeSchemaApiGrpcOptions.AdvertisedAuthSchemes"/>.
/// A host configures the schemes through options; with none configured (the
/// default) the advertisement is empty and clients fall back to manual or Basic
/// selection.
/// </summary>
internal sealed class OptionsLatticeSchemaApiAuthSchemeSource : ILatticeSchemaApiAuthSchemeSource
{
    private readonly IOptionsMonitor<LatticeSchemaApiGrpcOptions> _options;

    /// <summary>Initialises the source over the binding options.</summary>
    public OptionsLatticeSchemaApiAuthSchemeSource(IOptionsMonitor<LatticeSchemaApiGrpcOptions> options)
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
