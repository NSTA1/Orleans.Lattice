using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry.Grpc;

/// <summary>
/// The default <see cref="ILatticeTelemetryApiAuthSchemeSource"/>: builds the
/// advertisement from
/// <see cref="LatticeTelemetryApiGrpcOptions.AdvertisedAuthSchemes"/>. A host
/// configures the schemes through options; with none configured (the default) the
/// advertisement is empty and clients fall back to manual or Basic selection.
/// </summary>
internal sealed class OptionsLatticeTelemetryApiAuthSchemeSource : ILatticeTelemetryApiAuthSchemeSource
{
    private static readonly AuthSchemeAdvertisement EmptyAdvertisement = new();

    private readonly IOptionsMonitor<LatticeTelemetryApiGrpcOptions> _options;

    /// <summary>Initialises the source over the binding options.</summary>
    /// <param name="options">The binding options monitor.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public OptionsLatticeTelemetryApiAuthSchemeSource(IOptionsMonitor<LatticeTelemetryApiGrpcOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public AuthSchemeAdvertisement GetAdvertisement()
    {
        var configured = _options.CurrentValue.AdvertisedAuthSchemes;

        // The overwhelmingly common case is a host that advertises nothing; serve
        // it from a cached singleton so the unauthenticated probe allocates nothing.
        return configured.Count == 0
            ? EmptyAdvertisement
            : new AuthSchemeAdvertisement { Schemes = [.. configured] };
    }
}
