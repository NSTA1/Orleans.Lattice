using NSubstitute;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

[TestFixture]
public class CapabilityExplorerTenantOperatorGateTests
{
    private static IExplorerCapabilityStore StoreWith(bool authAdminAllowed)
    {
        var store = Substitute.For<IExplorerCapabilityStore>();
        store.Current.Returns(new ExplorerCapabilities { AuthAdminAllowed = authAdminAllowed });
        return store;
    }

    [Test]
    public async Task IsPlatformOperator_whenAuthAdminAllowed_isTrue()
    {
        var gate = new CapabilityExplorerTenantOperatorGate(StoreWith(authAdminAllowed: true));

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.True);
    }

    [Test]
    public async Task IsPlatformOperator_whenAuthAdminDenied_isFalse()
    {
        var gate = new CapabilityExplorerTenantOperatorGate(StoreWith(authAdminAllowed: false));

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public async Task IsPlatformOperator_emptyCapabilities_isFalse()
    {
        var store = Substitute.For<IExplorerCapabilityStore>();
        store.Current.Returns(ExplorerCapabilities.Empty);
        var gate = new CapabilityExplorerTenantOperatorGate(store);

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public void Ctor_nullStore_throws()
    {
        Assert.That(() => new CapabilityExplorerTenantOperatorGate(null!), Throws.ArgumentNullException);
    }
}
