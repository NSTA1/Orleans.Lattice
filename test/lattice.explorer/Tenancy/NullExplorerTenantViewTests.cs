using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

[TestFixture]
public class NullExplorerTenantViewTests
{
    private static readonly IExplorerTenantView View = NullExplorerTenantView.Instance;

    [Test]
    public void Instance_isSingleton()
    {
        Assert.That(NullExplorerTenantView.Instance, Is.SameAs(NullExplorerTenantView.Instance));
    }

    [Test]
    public void IsActive_isFalse()
    {
        Assert.That(View.IsActive, Is.False);
    }

    [Test]
    public void ActiveTenant_isNull()
    {
        Assert.That(View.ActiveTenant, Is.Null);
    }

    [Test]
    public async Task ResolveEffectiveVisibility_isAllTenants()
    {
        Assert.That(
            await View.ResolveEffectiveVisibilityAsync(),
            Is.EqualTo(ExplorerTenantVisibility.AllTenants));
    }

    [Test]
    public void IsVisible_isAlwaysTrue()
    {
        Assert.That(View.IsVisible(ExplorerTenantVisibility.ActiveTenant, "t/acme/orders"), Is.True);
    }

    [Test]
    public async Task ScopeAsync_returnsItemsUnchanged()
    {
        var items = new[] { "t/acme/orders", "t/globex/orders" };

        var scoped = await View.ScopeAsync<string>(items, static id => id);

        Assert.That(scoped, Is.SameAs(items));
    }
}
