using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// Coverage for the fail-closed default accessible-tenant source: it reports the
/// tenant the caller is already scoped to, and nothing else, so a deployment
/// that cannot enumerate tenants offers no way to reach one.
/// </summary>
/// <remarks>
/// Direct assertions against the type with a real per-circuit context - no
/// cluster, no timing, no ordering, no wall clock, no GC dependence.
/// </remarks>
[TestFixture]
public class ActiveTenantOnlyAccessibleTenantSourceTests
{
    [Test]
    public void Ctor_nullContext_throws()
    {
        Assert.That(
            () => new ActiveTenantOnlyAccessibleTenantSource(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task GetAccessibleTenantsAsync_noActiveTenant_isEmpty()
    {
        var source = new ActiveTenantOnlyAccessibleTenantSource(new ExplorerTenantContext());

        Assert.That(await source.GetAccessibleTenantsAsync(), Is.Empty);
    }

    [Test]
    public async Task GetAccessibleTenantsAsync_activeTenant_reportsOnlyThatTenant()
    {
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId) };
        var source = new ActiveTenantOnlyAccessibleTenantSource(context);

        Assert.That(
            await source.GetAccessibleTenantsAsync(),
            Is.EqualTo(new[] { new ExplorerTenantId(SampleTenant.TenantId) }));
    }

    [Test]
    public async Task GetAccessibleTenantsAsync_repeatedForOneTenant_reusesTheSameList()
    {
        // Asked on every tenant-control refresh, so the answer for an unchanged
        // scope must not allocate a fresh array each time.
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId) };
        var source = new ActiveTenantOnlyAccessibleTenantSource(context);

        var first = await source.GetAccessibleTenantsAsync();
        var second = await source.GetAccessibleTenantsAsync();

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public async Task GetAccessibleTenantsAsync_afterTheScopeChanges_reportsTheNewTenant()
    {
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId) };
        var source = new ActiveTenantOnlyAccessibleTenantSource(context);
        await source.GetAccessibleTenantsAsync();

        context.ActiveTenant = new ExplorerTenantId(SampleTenant.OtherTenantId);

        Assert.That(
            await source.GetAccessibleTenantsAsync(),
            Is.EqualTo(new[] { new ExplorerTenantId(SampleTenant.OtherTenantId) }));
    }

    [Test]
    public async Task GetAccessibleTenantsAsync_afterTheScopeIsCleared_isEmptyAgain()
    {
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId(SampleTenant.TenantId) };
        var source = new ActiveTenantOnlyAccessibleTenantSource(context);
        await source.GetAccessibleTenantsAsync();

        context.ActiveTenant = null;

        Assert.That(await source.GetAccessibleTenantsAsync(), Is.Empty);
    }
}
