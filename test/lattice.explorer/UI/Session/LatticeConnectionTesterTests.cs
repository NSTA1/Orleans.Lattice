using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.Tests.UI.Session;

/// <summary>
/// The connection tester's own guards: it dials only on a head that accepts
/// browser-driven endpoint configuration, anonymously, and it never sends the
/// operator's transport headers to an endpoint other than the configured one.
/// </summary>
[TestFixture]
public sealed class LatticeConnectionTesterTests
{
    private const string Configured = "https://cluster.example:443";

    private static readonly IReadOnlyDictionary<string, string> FrontDoor =
        new Dictionary<string, string> { ["X-Azure-FDID"] = "operator-origin-lock" };

    [Test]
    public void The_transport_headers_are_not_forwarded_to_a_different_endpoint()
    {
        var candidate = Candidate("https://10.0.0.5:8443");

        var settings = LatticeConnectionTester.ProbeSettings(candidate, Configured);

        Assert.Multiple(() =>
        {
            Assert.That(settings.Address, Is.EqualTo("https://10.0.0.5:8443"));
            Assert.That(settings.TransportHeaders, Is.Null, "the operator's origin-lock header must not reach a host the visitor chose");
        });
    }

    [Test]
    public void The_transport_headers_are_not_forwarded_when_no_endpoint_is_configured()
    {
        var settings = LatticeConnectionTester.ProbeSettings(Candidate(Configured), configuredEndpoint: null);

        Assert.That(settings.TransportHeaders, Is.Null);
    }

    [TestCase(Configured)]
    [TestCase("https://CLUSTER.example:443/")]
    [TestCase("  https://cluster.example:443  ")]
    public void The_transport_headers_are_kept_for_the_configured_endpoint(string endpoint)
    {
        var settings = LatticeConnectionTester.ProbeSettings(Candidate(endpoint), Configured);

        Assert.That(settings.TransportHeaders, Is.SameAs(FrontDoor));
    }

    [Test]
    public void The_probe_is_anonymous_even_for_the_configured_endpoint()
    {
        var candidate = Candidate(Configured) with { Headers = new Dictionary<string, string> { ["x-meta"] = "1" } };

        var settings = LatticeConnectionTester.ProbeSettings(candidate, Configured);

        Assert.That(settings.Authentication, Is.Null);
    }

    [Test]
    public void A_head_that_refuses_interactive_configuration_refuses_the_probe_before_dialling()
    {
        var explorer = new FakeExplorerSession(new FakeStateConnection()).Configured(new ExplorerConfiguration { Endpoint = Configured });
        var tester = new LatticeConnectionTester(explorer, new SessionEndpointConfigurationOptions());

        Assert.That(
            async () => await tester.TestAsync(Candidate("https://10.0.0.5:8443")),
            Throws.InvalidOperationException.With.Message.EqualTo(LatticeConnectionTester.RefusalMessage));
    }

    [Test]
    public void Test_and_probe_settings_reject_missing_arguments()
    {
        var tester = new LatticeConnectionTester(
            new FakeExplorerSession(new FakeStateConnection()),
            new SessionEndpointConfigurationOptions { AllowInteractiveEndpointConfiguration = true });

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => tester.TestAsync(null!));
            Assert.That(() => LatticeConnectionTester.ProbeSettings(null!, Configured), Throws.ArgumentNullException);
        });
    }

    private static ExplorerConfiguration Candidate(string endpoint) => new()
    {
        Endpoint = endpoint,
        TransportHeaders = FrontDoor,
    };
}
