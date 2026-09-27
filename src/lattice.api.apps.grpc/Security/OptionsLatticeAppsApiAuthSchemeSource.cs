using System.Collections.Immutable;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Apps.Grpc;

internal sealed class OptionsLatticeAppsApiAuthSchemeSource(
    IOptionsMonitor<LatticeAppsApiGrpcOptions> options) : ILatticeAppsApiAuthSchemeSource
{
    private readonly IOptionsMonitor<LatticeAppsApiGrpcOptions> _options =
        options ?? throw new ArgumentNullException(nameof(options));

    public AuthSchemeAdvertisement GetAdvertisement()
        => new() { Schemes = _options.CurrentValue.AdvertisedAuthSchemes.ToImmutableArray() };
}
