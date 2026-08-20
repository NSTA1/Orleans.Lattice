using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Api.Replication.Grpc.Tests;

/// <summary>
/// Unit tests for <see cref="OptionsLatticeReplicationApiAuthSchemeSource"/>: the
/// default advertisement is empty, and configured schemes are surfaced verbatim.
/// </summary>
public sealed class OptionsLatticeReplicationApiAuthSchemeSourceTests
{
    private static OptionsLatticeReplicationApiAuthSchemeSource CreateSource(
        LatticeReplicationApiGrpcOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationApiGrpcOptions>>();
        monitor.CurrentValue.Returns(options);
        return new OptionsLatticeReplicationApiAuthSchemeSource(monitor);
    }

    [Test]
    public void GetAdvertisement_with_no_configured_schemes_returns_empty()
    {
        var source = CreateSource(new LatticeReplicationApiGrpcOptions());

        var advertisement = source.GetAdvertisement();

        Assert.That(advertisement.Schemes, Is.Empty);
    }

    [Test]
    public void GetAdvertisement_returns_configured_schemes()
    {
        var options = new LatticeReplicationApiGrpcOptions();
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "basic", DisplayName = "Basic" });
        options.AdvertisedAuthSchemes.Add(new AuthSchemeDescriptor { SchemeId = "entra", DisplayName = "Entra" });
        var source = CreateSource(options);

        var advertisement = source.GetAdvertisement();

        Assert.Multiple(() =>
        {
            Assert.That(advertisement.Schemes, Has.Length.EqualTo(2));
            Assert.That(advertisement.Schemes[0].SchemeId, Is.EqualTo("basic"));
            Assert.That(advertisement.Schemes[1].SchemeId, Is.EqualTo("entra"));
        });
    }
}
