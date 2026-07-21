using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Connection;

/// <summary>
/// Covers the sign-in-independent <see cref="LatticeConnectionSettings.TransportHeaders"/>
/// seam: routing metadata (for example an origin-lock header) that must accompany
/// every call and must not be dropped when a sign-in replaces the authentication.
/// </summary>
[TestFixture]
public class TransportHeadersTests
{
    private static readonly IReadOnlyDictionary<string, string> Fdid =
        new Dictionary<string, string> { ["X-Azure-FDID"] = "front-door-id" };

    [Test]
    public void TransportHeaders_defaultsToNull()
    {
        var settings = new LatticeConnectionSettings { Address = "https://host:443" };

        Assert.That(settings.TransportHeaders, Is.Null);
    }

    [Test]
    public void TransportHeaders_surviveAuthenticationSwap()
    {
        var settings = new LatticeConnectionSettings
        {
            Address = "https://host:443",
            TransportHeaders = Fdid,
        };

        // Mirrors the sign-in path (ExplorerAuthSession), which replaces the whole
        // Authentication object via a record `with`. TransportHeaders must persist.
        var afterSignIn = settings with
        {
            Authentication = new LatticeCallAuthentication
            {
                Headers = new Dictionary<string, string> { ["authorization"] = "Bearer token" },
            },
        };

        Assert.That(afterSignIn.TransportHeaders, Is.SameAs(Fdid));
        Assert.That(afterSignIn.Authentication, Is.Not.Null);
    }

    [Test]
    public void ToConnectionSettings_mapsPopulatedTransportHeaders()
    {
        var config = new ExplorerConfiguration
        {
            Endpoint = "https://host:443",
            TransportHeaders = Fdid,
        };

        var settings = config.ToConnectionSettings();

        Assert.That(settings.TransportHeaders, Is.EqualTo(Fdid));
    }

    [Test]
    public void ToConnectionSettings_mapsEmptyTransportHeadersToNull()
    {
        var config = new ExplorerConfiguration
        {
            Endpoint = "https://host:443",
            TransportHeaders = new Dictionary<string, string>(),
        };

        Assert.That(config.ToConnectionSettings().TransportHeaders, Is.Null);
    }

    [Test]
    public void ToConnectionSettings_keepsTransportHeadersSeparateFromAuthentication()
    {
        var config = new ExplorerConfiguration
        {
            Endpoint = "https://host:443",
            TransportHeaders = Fdid,
        };

        var settings = config.ToConnectionSettings();

        // TransportHeaders must never leak into the credential surface.
        Assert.That(settings.Authentication, Is.Null);
        Assert.That(settings.TransportHeaders, Is.EqualTo(Fdid));
    }
}
