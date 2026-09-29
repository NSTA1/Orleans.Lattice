using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.Tests.UI.Transport;

/// <summary>
/// The circuit's asserted tenant: Core's tenant read live, nothing without
/// tenancy, and a pin that holds a tenant for one asynchronous flow and one
/// circuit only.
/// </summary>
[TestFixture]
public sealed class ShellAssertedTenantTests
{
    [Test]
    public void It_reads_the_core_provider_live_and_asserts_nothing_without_one()
    {
        var provider = new FakeActiveTenantProvider("acme");
        var tenant = new ShellAssertedTenant(provider);
        var before = tenant.AssertedTenant;

        provider.Set("globex");

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo("acme"));
            Assert.That(tenant.AssertedTenant, Is.EqualTo("globex"));
            Assert.That(ShellAssertedTenant.None.AssertedTenant, Is.Null);
            Assert.That(new ShellAssertedTenant().AssertedTenant, Is.Null);
        });
    }

    [Test]
    public void Same_compares_ordinally_and_treats_two_nones_as_the_same()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ShellAssertedTenant.Same(null, null), Is.True);
            Assert.That(ShellAssertedTenant.Same("acme", "acme"), Is.True);
            Assert.That(ShellAssertedTenant.Same("acme", "Acme"), Is.False);
            Assert.That(ShellAssertedTenant.Same("acme", null), Is.False);
        });
    }

    [Test]
    public void A_pin_holds_its_tenant_until_disposed_and_then_restores_the_live_one()
    {
        var provider = new FakeActiveTenantProvider("acme");
        var tenant = new ShellAssertedTenant(provider);

        string? pinned;
        string? nested;
        using (tenant.Pin("acme"))
        {
            provider.Set("globex");
            pinned = tenant.AssertedTenant;
            using (tenant.Pin(null))
            {
                nested = tenant.AssertedTenant;
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(pinned, Is.EqualTo("acme"));
            Assert.That(nested, Is.Null);
            Assert.That(tenant.AssertedTenant, Is.EqualTo("globex"));
        });
    }

    [Test]
    public void A_pin_is_honoured_only_by_the_circuit_that_set_it()
    {
        var mine = new ShellAssertedTenant(new FakeActiveTenantProvider("acme"));
        var theirs = new ShellAssertedTenant(new FakeActiveTenantProvider("globex"));

        using (mine.Pin("initech"))
        {
            Assert.Multiple(() =>
            {
                Assert.That(mine.AssertedTenant, Is.EqualTo("initech"));
                Assert.That(theirs.AssertedTenant, Is.EqualTo("globex"));
            });
        }
    }

    [Test]
    public async Task A_pin_set_inside_an_asynchronous_flow_never_leaks_to_its_caller()
    {
        var provider = new FakeActiveTenantProvider("acme");
        var tenant = new ShellAssertedTenant(provider);
        var entered = new TaskCompletionSource();
        var release = new TaskCompletionSource();

        var flow = Task.Run(async () =>
        {
            using var _ = tenant.Pin("acme");
            entered.SetResult();
            await release.Task;
            return tenant.AssertedTenant;
        });
        await entered.Task;
        provider.Set("globex");
        var outside = tenant.AssertedTenant;
        release.SetResult();

        Assert.Multiple(async () =>
        {
            Assert.That(outside, Is.EqualTo("globex"));
            Assert.That(await flow, Is.EqualTo("acme"));
        });
    }
}
