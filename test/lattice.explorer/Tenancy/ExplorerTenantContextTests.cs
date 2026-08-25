using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

[TestFixture]
public class ExplorerTenantContextTests
{
    [Test]
    public void Defaults_noActiveTenant_activeTenantScope()
    {
        var context = new ExplorerTenantContext();

        Assert.That(context.ActiveTenant, Is.Null);
        Assert.That(context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.ActiveTenant));
    }

    [Test]
    public void ActiveTenant_isSettable()
    {
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") };

        Assert.That(context.ActiveTenant, Is.EqualTo(new ExplorerTenantId("acme")));
    }

    [Test]
    public void RequestedVisibility_isSettable()
    {
        var context = new ExplorerTenantContext { RequestedVisibility = ExplorerTenantVisibility.AllTenants };

        Assert.That(context.RequestedVisibility, Is.EqualTo(ExplorerTenantVisibility.AllTenants));
    }
}
