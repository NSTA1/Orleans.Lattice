using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Telemetry;
using Orleans.Lattice.Explorer.Tests.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// Covers the controlled domain model a telemetry plugin is handed: the
/// operations surface it composes, the availability probe it forwards, and the
/// requested visibility it reads from the shell without deciding anything of its
/// own.
/// </summary>
[TestFixture]
public class TelemetryDomainTests
{
    private FakeTelemetryQueryClient _client = null!;
    private TelemetryQueryService _queries = null!;
    private TelemetryAvailability _availability = null!;

    [SetUp]
    public void SetUp()
    {
        _client = new FakeTelemetryQueryClient();
        _queries = new TelemetryQueryService(_client);
        _availability = new TelemetryAvailability(_queries);
    }

    private TelemetryDomain Create(StubTenantSwitcher? switcher = null) =>
        new(_queries, _availability, switcher);

    [Test]
    public void Constructor_rejects_a_null_operations_surface() =>
        Assert.That(() => new TelemetryDomain(null!, _availability), Throws.ArgumentNullException);

    [Test]
    public void Constructor_rejects_a_null_availability_probe() =>
        Assert.That(() => new TelemetryDomain(_queries, null!), Throws.ArgumentNullException);

    [Test]
    public void The_operations_surface_is_the_one_it_was_built_with() =>
        Assert.That(Create().Queries, Is.SameAs(_queries));

    [Test]
    public void Without_a_tenant_view_the_domain_reports_tenancy_disabled_and_the_narrowest_request() =>
        Assert.Multiple(() =>
        {
            Assert.That(Create().IsTenancyEnabled, Is.False);
            Assert.That(Create().RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
        });

    [Test]
    public void An_inactive_tenant_view_also_reports_the_narrowest_request()
    {
        var switcher = new StubTenantSwitcher(isActive: false)
        {
            RequestedVisibility = ExplorerTenantVisibility.AllTenants,
        };

        var domain = Create(switcher);

        Assert.Multiple(() =>
        {
            Assert.That(domain.IsTenancyEnabled, Is.False);
            Assert.That(
                domain.RequestedVisibility,
                Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant),
                "a head with tenant scoping switched off has nothing wider to ask for");
        });
    }

    [Test]
    public void The_head_requested_visibility_is_carried_across()
    {
        var switcher = new StubTenantSwitcher
        {
            RequestedVisibility = ExplorerTenantVisibility.AllTenants,
        };

        var domain = Create(switcher);

        Assert.Multiple(() =>
        {
            Assert.That(domain.IsTenancyEnabled, Is.True);
            Assert.That(domain.RequestedVisibility, Is.EqualTo(ExplorerTelemetryVisibility.AllTenants));
        });
    }

    [Test]
    public void Every_head_visibility_maps_onto_its_telemetry_counterpart()
    {
        (ExplorerTenantVisibility Head, ExplorerTelemetryVisibility Expected)[] cases =
        [
            (ExplorerTenantVisibility.ActiveTenant, ExplorerTelemetryVisibility.ActiveTenant),
            (ExplorerTenantVisibility.AllTenants, ExplorerTelemetryVisibility.AllTenants),
        ];

        Assert.Multiple(() =>
        {
            foreach (var (head, expected) in cases)
            {
                var switcher = new StubTenantSwitcher { RequestedVisibility = head };
                Assert.That(Create(switcher).RequestedVisibility, Is.EqualTo(expected));
            }
        });
    }

    [Test]
    public async Task The_availability_probe_is_forwarded()
    {
        var access = await Create().ProbeAvailabilityAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
            Assert.That(_client.CatalogCallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_absent_facade_reaches_the_plugin_gate_as_unavailable()
    {
        _client.CatalogThrows = new TelemetryUnavailableException();

        var access = await Create().ProbeAvailabilityAsync();

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.IsVisible, Is.False);
        });
    }

    [Test]
    public async Task A_panel_reaches_the_cluster_only_through_the_operations_surface()
    {
        var domain = Create();

        var catalog = await domain.Queries.GetCatalogAsync();
        var evaluated = await domain.Queries.QueryAsync(
            ExplorerTelemetryRequest.For(SampleTelemetry.RangeQueryId));

        Assert.Multiple(() =>
        {
            Assert.That(catalog.IsSuccess, Is.True);
            Assert.That(evaluated.IsSuccess, Is.True);
            Assert.That(evaluated.Value!.QueryId, Is.EqualTo(SampleTelemetry.RangeQueryId));
        });
    }
}
