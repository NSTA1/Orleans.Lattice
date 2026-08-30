using Grpc.Core;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Covers the seam's availability detection: every "there is nothing here"
/// path resolves to the four-state model's
/// <see cref="ExplorerPluginAccessState.Unavailable"/> so a telemetry plugin
/// renders nothing, and no fault escapes a probe as an exception.
/// </summary>
[TestFixture]
public class TelemetryAvailabilityTests
{
    private FakeTelemetryQueryClient _client = null!;

    [SetUp]
    public void SetUp() => _client = new FakeTelemetryQueryClient();

    private TelemetryAvailability Create() => new(new TelemetryQueryService(_client));

    private static IExplorerPluginHostContext ConnectedContext()
    {
        var host = PluginTestHost.Create();
        host.State.Connection = new ExplorerPluginConnectionStatus(ExplorerPluginConnectionState.Connected);
        return host.Contexts.Create("telemetry");
    }

    [Test]
    public void Constructor_rejects_a_null_operations_surface() =>
        Assert.That(() => new TelemetryAvailability(null!), Throws.ArgumentNullException);

    [Test]
    public async Task A_cluster_offering_queries_is_allowed()
    {
        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(access.IsAllowed, Is.True);
            Assert.That(_client.CatalogCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_absent_telemetry_facade_reports_unavailable_rather_than_an_error()
    {
        _client.CatalogThrows = new TelemetryUnavailableException("the telemetry facade is not registered");

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.IsVisible, Is.False, "an unavailable surface renders no entry at all");
            Assert.That(access.Reason, Is.EqualTo("the telemetry facade is not registered"));
        });
    }

    [Test]
    public async Task An_untranslated_unimplemented_status_also_reports_unavailable()
    {
        _client.CatalogThrows = new RpcException(new Status(StatusCode.Unimplemented, "not served here"));

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.Reason, Is.EqualTo("not served here"));
        });
    }

    [Test]
    public async Task An_unreachable_facade_reports_unavailable()
    {
        // Reading the catalogue never fails for a backend fault - the facade
        // degrades to an empty catalogue instead - so a transport failure on this
        // one call means the endpoint could not be reached at all.
        _client.CatalogThrows = new RpcException(new Status(StatusCode.Unavailable, "connection refused"));

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.Reason, Is.EqualTo("connection refused"));
        });
    }

    [Test]
    public async Task An_empty_catalogue_reports_unavailable_and_never_accuses_the_caller()
    {
        // An empty catalogue means either "no backend configured" or "you may run
        // none of these", and the facade makes the two indistinguishable on
        // purpose. Guessing "denied" would accuse a caller that may be entirely
        // entitled; reporting "nothing to render" is true either way.
        _client.CatalogResult = TelemetryQueryCatalog.Empty;

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.State, Is.Not.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.IsVisible, Is.False);
            Assert.That(access.Reason, Is.Not.Empty);
        });
    }

    [Test]
    public async Task A_refused_caller_is_denied_and_still_visible()
    {
        _client.CatalogThrows = new LatticeAuthorizationDeniedException("not yours");

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.IsVisible, Is.True, "a denial greys out rather than hides");
            Assert.That(access.Reason, Is.EqualTo("not yours"));
        });
    }

    [Test]
    public async Task A_caller_with_no_credential_is_asked_to_sign_in()
    {
        _client.CatalogThrows = new RpcException(new Status(StatusCode.Unauthenticated, "sign in"));

        var access = await Create().ProbeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(access.IsVisible, Is.True);
        });
    }

    [Test]
    public async Task An_unconfigured_endpoint_is_denied_rather_than_throwing()
    {
        _client.CatalogThrows = new InvalidOperationException("The explorer is not configured with an endpoint yet.");

        var access = await Create().ProbeAsync();

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
    }

    [Test]
    public async Task A_probe_never_throws_whatever_the_fault()
    {
        Exception[] faults =
        [
            new TelemetryUnavailableException(),
            new LatticeAuthorizationDeniedException("no"),
            new RpcException(new Status(StatusCode.Unauthenticated, "sign in")),
            new RpcException(new Status(StatusCode.Internal, "boom")),
            new InvalidOperationException("unconfigured"),
        ];

        foreach (var fault in faults)
        {
            _client = new FakeTelemetryQueryClient { CatalogThrows = fault };
            var access = await Create().ProbeAsync();
            Assert.That(access.Reason, Is.Not.Null, $"{fault.GetType().Name} must resolve to a stated access state");
        }
    }

    [Test]
    public void The_gate_form_rejects_a_null_context() =>
        Assert.That(async () => await Create().ProbeAsync(null!), Throws.ArgumentNullException);

    [Test]
    public async Task The_gate_form_reports_unavailable_while_the_shell_is_disconnected()
    {
        var host = PluginTestHost.Create();
        host.State.Connection = ExplorerPluginConnectionStatus.Disconnected;

        var access = await Create().ProbeAsync(host.Contexts.Create("telemetry"));

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(_client.CatalogCallCount, Is.Zero, "no call is worth making without a connection");
        });
    }

    [Test]
    public async Task The_gate_form_probes_the_cluster_once_connected()
    {
        var access = await Create().ProbeAsync(ConnectedContext());

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(_client.CatalogCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_gate_form_probes_while_reconnecting_within_the_grace_window()
    {
        var host = PluginTestHost.Create();
        host.State.Connection = new ExplorerPluginConnectionStatus(ExplorerPluginConnectionState.Reconnecting);

        var access = await Create().ProbeAsync(host.Contexts.Create("telemetry"));

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(_client.CatalogCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task The_gate_form_reports_unavailable_for_an_absent_facade_on_a_live_connection()
    {
        _client.CatalogThrows = new TelemetryUnavailableException();

        var access = await Create().ProbeAsync(ConnectedContext());

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
    }

    [Test]
    public async Task A_probe_reuses_the_remembered_catalogue_rather_than_re_reading_it()
    {
        var availability = Create();

        await availability.ProbeAsync();
        await availability.ProbeAsync();

        Assert.That(_client.CatalogCallCount, Is.EqualTo(1));
    }
}
