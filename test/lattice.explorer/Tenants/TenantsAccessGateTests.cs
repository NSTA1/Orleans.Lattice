using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenants;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The Tenants plugin's four-state gate: the surface exists only where the
/// cluster serves tenancy, and only a validated platform operator may use it.
/// </summary>
[TestFixture]
public sealed class TenantsAccessGateTests
{
    private static IExplorerPluginHostContext Context() =>
        PluginTestHost.Context(TenantsPluginKeys.PluginId);

    [Test]
    public void Constructor_null_domain_throws()
    {
        Assert.That(() => new TenantsAccessGate(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Probe_null_context_throws()
    {
        var gate = new TenantsAccessGate(new FakeTenancyDomain());

        Assert.That(
            async () => await gate.ProbeAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Probe_operator_on_a_tenancy_cluster_is_allowed()
    {
        var domain = new FakeTenancyDomain { Availability = ExplorerPluginAccess.Allowed, IsOperator = true };
        var gate = new TenantsAccessGate(domain);

        var access = await gate.ProbeAsync(Context());

        Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Allowed));
    }

    [Test]
    public async Task Probe_non_operator_on_a_tenancy_cluster_is_denied_but_visible()
    {
        var domain = new FakeTenancyDomain { Availability = ExplorerPluginAccess.Allowed, IsOperator = false };
        var gate = new TenantsAccessGate(domain);

        var access = await gate.ProbeAsync(Context());

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.Reason, Is.EqualTo(TenantsAccessGate.NotOperatorReason));

            // A denial greys the area out rather than hiding it, so a caller can
            // see that a tenant surface exists and is not theirs.
            Assert.That(access.IsVisible, Is.True);
        });
    }

    [Test]
    public async Task Probe_reports_unavailable_when_the_cluster_serves_no_tenancy()
    {
        var domain = new FakeTenancyDomain
        {
            Availability = ExplorerPluginAccess.ReportUnavailable("no tenancy add-on"),
            IsOperator = true,
        };

        var gate = new TenantsAccessGate(domain);

        var access = await gate.ProbeAsync(Context());

        Assert.Multiple(() =>
        {
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Unavailable));
            Assert.That(access.Reason, Is.EqualTo("no tenancy add-on"));

            // The shell renders no entry at all, which is the whole of D9.
            Assert.That(access.IsVisible, Is.False);
        });
    }

    [Test]
    public async Task Probe_does_not_check_the_operator_when_tenancy_is_absent()
    {
        var domain = new FakeTenancyDomain
        {
            Availability = ExplorerPluginAccess.Unavailable,
            IsOperator = true,
        };

        var gate = new TenantsAccessGate(domain);

        await gate.ProbeAsync(Context());

        // Probing the operator first would let a non-tenant cluster render a
        // greyed-out entry to a non-operator, which is the one thing the
        // unavailable state exists to prevent.
        Assert.Multiple(() =>
        {
            Assert.That(domain.AvailabilityProbes, Is.EqualTo(1));
            Assert.That(domain.OperatorProbes, Is.Zero);
        });
    }

    [Test]
    public async Task Probe_passes_authentication_required_through_unchanged()
    {
        var domain = new FakeTenancyDomain
        {
            Availability = ExplorerPluginAccess.RequireAuthentication("sign in"),
            IsOperator = true,
        };

        var gate = new TenantsAccessGate(domain);

        var access = await gate.ProbeAsync(Context());

        Assert.Multiple(() =>
        {
            // Recoverable, so the shell offers a sign-in rather than an inert
            // grey-out.
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.AuthenticationRequired));
            Assert.That(access.Reason, Is.EqualTo("sign in"));
            Assert.That(domain.OperatorProbes, Is.Zero);
        });
    }

    [Test]
    public async Task Probe_passes_a_denied_availability_through_unchanged()
    {
        var domain = new FakeTenancyDomain
        {
            Availability = ExplorerPluginAccess.Deny("unreachable"),
            IsOperator = true,
        };

        var gate = new TenantsAccessGate(domain);

        var access = await gate.ProbeAsync(Context());

        Assert.Multiple(() =>
        {
            // A dropped connection is a denial, never unavailable: hiding the
            // whole surface on a reconnect would look like an uninstall.
            Assert.That(access.State, Is.EqualTo(ExplorerPluginAccessState.Denied));
            Assert.That(access.Reason, Is.EqualTo("unreachable"));
            Assert.That(access.IsVisible, Is.True);
        });
    }

    [Test]
    public async Task Probe_default_access_fails_closed()
    {
        // default(ExplorerPluginAccess) is Denied with no reason, so an unprobed
        // or defaulted availability can never admit a caller.
        var domain = new FakeTenancyDomain { Availability = default, IsOperator = true };
        var gate = new TenantsAccessGate(domain);

        var access = await gate.ProbeAsync(Context());

        Assert.That(access.IsAllowed, Is.False);
    }
}
