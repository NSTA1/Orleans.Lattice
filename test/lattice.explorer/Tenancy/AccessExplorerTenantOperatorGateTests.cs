using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// The Explorer's cross-tenant root of trust after the plugin conversion: the
/// operator gate reads the Access plugin's own published decision from the keyed
/// access store rather than a shared capability record every area also writes.
/// </summary>
[TestFixture]
public class AccessExplorerTenantOperatorGateTests
{
    private static ExplorerPluginAccessStore StoreWith(ExplorerPluginAccess access)
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(AccessPluginKeys.PluginId, access);
        return store;
    }

    [Test]
    public async Task IsPlatformOperator_whenTheAccessPluginIsAllowed_isTrue()
    {
        var gate = new AccessExplorerTenantOperatorGate(StoreWith(ExplorerPluginAccess.Allowed));

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.True);
    }

    [Test]
    public async Task IsPlatformOperator_whenTheAccessPluginIsDenied_isFalse()
    {
        var gate = new AccessExplorerTenantOperatorGate(StoreWith(ExplorerPluginAccess.Denied));

        Assert.That(await gate.IsPlatformOperatorAsync(), Is.False);
    }

    [Test]
    public async Task IsPlatformOperator_whenASignInIsRequired_isFalse()
    {
        var gate = new AccessExplorerTenantOperatorGate(StoreWith(ExplorerPluginAccess.AuthenticationRequired));

        Assert.That(
            await gate.IsPlatformOperatorAsync(),
            Is.False,
            "only an outright allow admits an operator; a recoverable state is not an admission");
    }

    [Test]
    public async Task IsPlatformOperator_unprobedStore_isFalse()
    {
        var gate = new AccessExplorerTenantOperatorGate(new ExplorerPluginAccessStore());

        Assert.That(
            await gate.IsPlatformOperatorAsync(),
            Is.False,
            "an unprobed key reads as denied, so the gate is fail-closed before any probe runs");
    }

    [Test]
    public async Task IsPlatformOperator_readsOnlyTheAccessPluginsOwnKey()
    {
        var store = new ExplorerPluginAccessStore();
        store.Set("some.other.plugin", ExplorerPluginAccess.Allowed);
        store.Set(AccessPluginKeys.PluginId, AccessPluginKeys.DirectoryScope, ExplorerPluginAccess.Allowed);
        var gate = new AccessExplorerTenantOperatorGate(store);

        Assert.That(
            await gate.IsPlatformOperatorAsync(),
            Is.False,
            "neither a sibling plugin nor this plugin's scoped sub-capability may admit an operator");
    }

    [Test]
    public void Ctor_nullStore_throws()
    {
        Assert.That(() => new AccessExplorerTenantOperatorGate(null!), Throws.ArgumentNullException);
    }
}
