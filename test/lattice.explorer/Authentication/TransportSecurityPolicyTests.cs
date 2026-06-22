using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.Tests.Configuration;

[TestFixture]
public class TransportSecurityPolicyTests
{
    [Test]
    public void TryValidateEndpoint_loopbackHttp_secureMode_isAllowed()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateEndpoint("http://localhost:5199", ExplorerTransportMode.Secure, out var error),
            Is.True);
        Assert.That(error, Is.Null);
    }

    [Test]
    public void TryValidateEndpoint_nonLoopbackHttp_secureMode_requiresTls()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateEndpoint("http://cluster.internal:8080", ExplorerTransportMode.Secure, out var error),
            Is.False);
        Assert.That(error, Is.Not.Null);
    }

    [Test]
    public void TryValidateEndpoint_nonLoopbackHttps_secureMode_isAllowed()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateEndpoint("https://cluster.internal:443", ExplorerTransportMode.Secure, out var error),
            Is.True);
        Assert.That(error, Is.Null);
    }

    [Test]
    public void TryValidateEndpoint_loopback_insecureDevMode_isAllowed()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateEndpoint("http://127.0.0.1:5199", ExplorerTransportMode.InsecureLoopbackDev, out var error),
            Is.True);
        Assert.That(error, Is.Null);
    }

    [Test]
    public void TryValidateEndpoint_nonLoopback_insecureDevMode_isRejected()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateEndpoint("http://cluster.internal:8080", ExplorerTransportMode.InsecureLoopbackDev, out var error),
            Is.False);
        Assert.That(error, Is.Not.Null);
    }

    [Test]
    public void TryValidateEndpoint_invalidUrl_isRejected()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateEndpoint("not-a-url", ExplorerTransportMode.Secure, out var error),
            Is.False);
        Assert.That(error, Is.Not.Null);
    }

    [Test]
    public void TryValidateConnection_secureMode_nonLoopback_anonymous_isRejected()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateConnection("https://cluster.internal:443", ExplorerTransportMode.Secure, hasCredential: false, out var error),
            Is.False);
        Assert.That(error, Is.Not.Null);
    }

    [Test]
    public void TryValidateConnection_secureMode_nonLoopback_authenticated_isAllowed()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateConnection("https://cluster.internal:443", ExplorerTransportMode.Secure, hasCredential: true, out var error),
            Is.True);
        Assert.That(error, Is.Null);
    }

    [Test]
    public void TryValidateConnection_secureMode_loopback_anonymous_isAllowed()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateConnection("http://localhost:5199", ExplorerTransportMode.Secure, hasCredential: false, out var error),
            Is.True);
        Assert.That(error, Is.Null);
    }

    [Test]
    public void TryValidateConnection_insecureDevMode_loopback_anonymous_isAllowed()
    {
        Assert.That(
            TransportSecurityPolicy.TryValidateConnection("http://localhost:5199", ExplorerTransportMode.InsecureLoopbackDev, hasCredential: false, out var error),
            Is.True);
        Assert.That(error, Is.Null);
    }
}
