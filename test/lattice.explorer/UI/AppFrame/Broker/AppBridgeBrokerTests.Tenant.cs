using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.UI.Framing;
using Orleans.Lattice.Explorer.UI.Framing.Broker;
using static Orleans.Lattice.Explorer.Tests.UI.Framing.Broker.BrokerHarness;

namespace Orleans.Lattice.Explorer.Tests.UI.Framing.Broker;

/// <summary>
/// A frame's bridge calls run in the tenant its app was launched in: the launch
/// records that tenant, and once the circuit asserts any other the broker closes
/// the frame rather than relay a read or write into the wrong tenant.
/// </summary>
public sealed partial class AppBridgeBrokerTests
{
    [Test]
    public async Task A_launch_records_the_tenant_it_was_authorised_in()
    {
        var harness = await CreateAsync(tenant: new FakeActiveTenantProvider("acme"));

        Assert.That(harness.Session.Launch.Tenant, Is.EqualTo("acme"));
    }

    [Test]
    public async Task A_request_in_the_launch_tenant_is_relayed()
    {
        var harness = await CreateAsync(tenant: new FakeActiveTenantProvider("acme"));

        AssertOk(await harness.SendAsync(1, "data.read", OrdersGet), 1);

        Assert.That(harness.Bridge!.Calls, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task Once_the_circuit_asserts_another_tenant_the_frame_is_closed_and_nothing_is_relayed()
    {
        var tenant = new FakeActiveTenantProvider("acme");
        var harness = await CreateAsync(tenant: tenant);

        tenant.Set("globex");
        var outcome = await harness.SendAsync(1, "data.read", OrdersGet);
        var after = await harness.SendAsync(2, "data.read", OrdersGet);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Effect, Is.EqualTo(AppBridgeEffect.Revoked));
            Assert.That(outcome.Argument, Is.EqualTo(AppFrameProtocol.RevokedClosed));
            Assert.That(Reply(outcome, 1).GetProperty("error").GetProperty("code").GetString(), Is.EqualTo(AppFrameProtocol.ErrorUnavailable));
            Assert.That(harness.Session.IsClosed, Is.True);
            Assert.That(after, Is.EqualTo(AppBridgeOutcome.Dropped), "a closed session answers nothing");
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains(nameof(AppBridgeDenial.TenantChanged)));
        });
    }

    [Test]
    public async Task Moving_to_no_tenant_closes_a_frame_launched_in_one()
    {
        var tenant = new FakeActiveTenantProvider("acme");
        var harness = await CreateAsync(tenant: tenant);

        tenant.Set(null);
        var outcome = await harness.SendAsync(1, "data.read", OrdersGet);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Effect, Is.EqualTo(AppBridgeEffect.Revoked));
            Assert.That(harness.Bridge!.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task Without_tenancy_a_launch_records_no_tenant_and_relays()
    {
        var harness = await CreateAsync();

        AssertOk(await harness.SendAsync(1, "data.read", OrdersGet), 1);

        Assert.That(harness.Session.Launch.Tenant, Is.Null);
    }
}
