using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// The default <see cref="ILatticeTenantAdminApiAuthSchemeSource"/>: builds the
/// advertisement from
/// <see cref="LatticeTenantAdminApiGrpcOptions.AdvertisedAuthSchemes"/>. A host
/// configures the schemes through options; with none configured (the default) the
/// advertisement is empty and clients fall back to manual or Basic selection.
/// </summary>
internal sealed class OptionsLatticeTenantAdminApiAuthSchemeSource : ILatticeTenantAdminApiAuthSchemeSource
{
    private readonly IOptionsMonitor<LatticeTenantAdminApiGrpcOptions> _options;

    /// <summary>Initialises the source over the binding options.</summary>
    public OptionsLatticeTenantAdminApiAuthSchemeSource(IOptionsMonitor<LatticeTenantAdminApiGrpcOptions> options)
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
