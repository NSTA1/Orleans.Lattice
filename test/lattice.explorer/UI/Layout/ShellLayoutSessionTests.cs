using Bunit;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Session;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// An area's availability follows the circuit's session: the layout loads the
/// persisted configuration and stored credential before it first asks, and asks
/// again whenever the sign-in, the configuration or the connection state changes
/// (issue #3831: areas reported "not connected" under a header that said
/// Connected, because their probes ran before the session was ready and were
/// never asked again).
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutSessionTests : ShellLayoutTestContext
{
    private const string NotConnected = "Connect to a cluster to browse its data.";
    private const string NotSignedIn = "Sign in to browse this cluster's data.";

    [Test]
    public void The_session_is_initialised_before_an_area_is_first_asked()
    {
        AddArea(new FakeArea("data", "Data")
        {
            Availability = _ => ValueTask.FromResult(Explorer.Initializations > 0
                ? AreaAvailability.Visible
                : AreaAvailability.Unavailable(NotConnected)),
        });
        Navigation.NavigateTo("data");

        var cut = RenderLayout();

        Assert.Multiple(() =>
        {
            Assert.That(Explorer.Initializations, Is.EqualTo(1));
            Assert.That(Auth.Initializations, Is.EqualTo(1));
            Assert.That(cut.Find("main").TextContent, Does.Not.Contain(NotConnected));
        });
    }

    [Test]
    public void Signing_in_asks_every_area_again()
    {
        AddArea(new FakeArea("data", "Data")
        {
            Availability = _ => ValueTask.FromResult(Auth.IsAuthenticated
                ? AreaAvailability.Visible
                : AreaAvailability.Unavailable(NotSignedIn)),
        });
        Navigation.NavigateTo("data");
        var cut = RenderLayout();
        Assert.That(cut.Find("main").TextContent, Does.Contain(NotSignedIn));

        Auth.SignIn("explorer-admin");

        cut.WaitForAssertion(() => Assert.That(cut.Find("main").TextContent, Does.Not.Contain(NotSignedIn)));
    }

    [Test]
    public void A_configuration_change_asks_every_area_again()
    {
        var configured = false;
        AddArea(new FakeArea("data", "Data")
        {
            Availability = _ => ValueTask.FromResult(configured
                ? AreaAvailability.Visible
                : AreaAvailability.Unavailable(NotConnected)),
        });
        Navigation.NavigateTo("data");
        var cut = RenderLayout();
        Assert.That(cut.Find("main").TextContent, Does.Contain(NotConnected));

        configured = true;
        _ = Explorer.ApplyAsync(SessionTestContext.RemoteConfiguration());

        cut.WaitForAssertion(() => Assert.That(cut.Find("main").TextContent, Does.Not.Contain(NotConnected)));
    }

    [Test]
    public void A_connection_state_change_asks_every_area_again_and_a_repeated_state_does_not()
    {
        var connection = new FakeStateConnection();
        connection.Seed(LatticeConnectionStatus.Disconnected);
        Services.AddSingleton<IExplorerSession>(new FakeExplorerSession(connection).Configured(SessionTestContext.RemoteConfiguration()));
        var area = AddArea(new FakeArea("data", "Data")
        {
            Availability = _ => ValueTask.FromResult(connection.Connection.Status.IsDisconnected
                ? AreaAvailability.Unavailable(NotConnected)
                : AreaAvailability.Visible),
        });
        Navigation.NavigateTo("data");
        var cut = RenderLayout();
        Assert.That(cut.Find("main").TextContent, Does.Contain(NotConnected));

        var connected = new LatticeConnectionStatus(LatticeConnectionState.Connected, "http://localhost:5199", "Connected.");
        connection.Move(connected);
        cut.WaitForAssertion(() => Assert.That(cut.Find("main").TextContent, Does.Not.Contain(NotConnected)));

        var asked = area.AvailabilityCalls;
        connection.Move(connected with { Message = "Healthy." });

        Assert.That(area.AvailabilityCalls, Is.EqualTo(asked), "a health report that changes no state asks nothing");
    }

    [Test]
    public void The_tenant_identity_is_resolved_before_an_area_is_first_asked_and_again_after_a_sign_in()
    {
        var resolver = new CountingTenantResolver();
        Services.AddSingleton<Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantIdentityResolver>(resolver);
        AddArea(new FakeArea("data", "Data")
        {
            Availability = _ => ValueTask.FromResult(resolver.Calls > 0
                ? AreaAvailability.Visible
                : AreaAvailability.Unavailable(NotConnected)),
        });
        Navigation.NavigateTo("data");

        var cut = RenderLayout();
        var first = resolver.Calls;
        Auth.SignIn("explorer-admin");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(1));
            Assert.That(cut.Find("main").TextContent, Does.Not.Contain(NotConnected));
        });
        cut.WaitForAssertion(() => Assert.That(resolver.Calls, Is.EqualTo(2), "a new identity is mapped onto its tenant again"));
    }

    [Test]
    public async Task Disposing_the_layout_stops_listening_to_the_session()
    {
        var cut = RenderLayout();
        Assert.That(Auth.AuthenticationSubscribers, Is.GreaterThan(0));
        var configurationSubscribers = Explorer.ConfigurationSubscribers;

        await cut.Instance.DisposeAsync();

        Assert.That(Explorer.ConfigurationSubscribers, Is.LessThan(configurationSubscribers));
    }

    private FakeAuthSession Auth => (FakeAuthSession)Services.GetRequiredService<IExplorerAuthSession>();

    private sealed class CountingTenantResolver : Orleans.Lattice.Explorer.Core.Tenancy.IExplorerTenantIdentityResolver
    {
        public int Calls { get; private set; }

        public ValueTask ResolveAsync(CancellationToken cancellationToken = default)
        {
            Calls++;
            return ValueTask.CompletedTask;
        }
    }
}
