using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <see cref="AppRegistry"/> refuses, at write time, an install or re-binding whose role binding names a
/// group outside the installing tenant, so the call itself fails closed.
/// </summary>
public sealed partial class AppRegistryTests
{
    [Test]
    public async Task InstallAsync_accepts_a_binding_to_the_installing_tenants_own_group()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        var result = await registry.InstallAsync(AppRegistryTestData.Request(
            tenant: AppRegistryTestData.Acme, bindings: [AppRoleBinding.Create("reader", "t/acme/readers")]));

        Assert.That(result.Succeeded, Is.True, result.Message);
        Assert.That(result.Record!.RoleBindings.Single().GroupId, Is.EqualTo("t/acme/readers"));
    }

    [TestCase("t/globex/readers")]
    [TestCase("t/default/readers")]
    [TestCase("t/acme/")]
    public void InstallAsync_refuses_a_binding_to_a_group_outside_the_installing_tenant(string groupId)
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);

        var ex = Assert.ThrowsAsync<ArgumentException>(() => registry.InstallAsync(AppRegistryTestData.Request(
            tenant: AppRegistryTestData.Acme, bindings: [AppRoleBinding.Create("reader", groupId)])));

        Assert.That(ex!.Message, Does.Contain(groupId));
        Assert.That(store.Peek("acme/notes"), Is.Null, "nothing is recorded");
    }

    [Test]
    public void InstallAsync_with_tenancy_off_refuses_any_tenant_group_binding()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        Assert.ThrowsAsync<ArgumentException>(() => registry.InstallAsync(AppRegistryTestData.Request(
            bindings: [AppRoleBinding.Create("reader", "t/acme/readers")])));
    }

    [Test]
    public async Task UpgradeAsync_refuses_a_rebinding_to_another_tenants_group_and_keeps_the_record()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        var installed = (await registry.InstallAsync(AppRegistryTestData.Request(tenant: AppRegistryTestData.Acme))).Record!;

        Assert.ThrowsAsync<ArgumentException>(() => registry.UpgradeAsync(AppRegistryTestData.Request(
            tenant: AppRegistryTestData.Acme, bindings: [AppRoleBinding.Create("reader", "t/globex/readers")]) with
        {
            ExpectedVersion = installed.Version,
            ExpectedRevision = installed.Revision,
        }));

        Assert.That(store.Peek("acme/notes"), Is.SameAs(installed));
    }

    [Test]
    public async Task InstallAsync_accepts_a_cluster_group_binding_under_a_tenant()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        var result = await registry.InstallAsync(AppRegistryTestData.Request(
            tenant: AppRegistryTestData.Acme, ceiling: AppCapabilityCeiling.Structural(LatticeOperation.Read)));

        Assert.That(result.Succeeded, Is.True, result.Message);
    }
}
