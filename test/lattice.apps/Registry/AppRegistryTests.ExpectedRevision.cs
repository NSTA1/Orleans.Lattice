namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <see cref="AppRegistryInstallRequest.ExpectedRevision"/>: a re-consent that carries fields
/// over from its read (the bindings a consent update keeps, the ceiling a role re-binding
/// keeps) is applied only while the record is still at the revision it read, so it can never
/// write back a value another transition replaced in between.
/// </summary>
public sealed partial class AppRegistryTests
{
    [Test]
    public async Task UpgradeAsync_with_a_stale_expected_revision_is_rejected_and_changes_nothing()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        var installed = (await registry.InstallAsync(AppRegistryTestData.Request())).Record!;
        var enabled = await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        Assert.That(enabled.Record!.Revision, Is.GreaterThan(installed.Revision));

        var stale = await registry.UpgradeAsync(AppRegistryTestData.Request() with
        {
            RoleBindings = [AppRoleBinding.Create("reader", "someone-else")],
            ExpectedVersion = AppRegistryTestData.V1,
            ExpectedRevision = installed.Revision,
        });

        Assert.Multiple(() =>
        {
            Assert.That(stale.Succeeded, Is.False);
            Assert.That(stale.Error, Is.EqualTo(AppRegistryTransitionError.ConcurrencyConflict));
            Assert.That(stale.Changed, Is.False);
            Assert.That(store.Peek(DefaultKey)!.Revision, Is.EqualTo(enabled.Record.Revision));
            Assert.That(store.Peek(DefaultKey)!.RoleBindings, Is.EqualTo(installed.RoleBindings));
        });
    }

    [Test]
    public async Task UpgradeAsync_with_the_current_expected_revision_replaces_the_bindings_and_keeps_the_state()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        await registry.InstallAsync(AppRegistryTestData.Request());
        var enabled = (await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug)).Record!;

        var result = await registry.UpgradeAsync(AppRegistryTestData.Request() with
        {
            RoleBindings = [AppRoleBinding.Create("reader", "new-readers")],
            ExpectedVersion = AppRegistryTestData.V1,
            ExpectedRevision = enabled.Revision,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.True, result.Message);
            Assert.That(result.Record!.Revision, Is.EqualTo(enabled.Revision + 1));
            Assert.That(result.Record.State, Is.EqualTo(AppRegistryLifecycleState.Enabled));
            Assert.That(result.Record.RoleBindings, Is.EqualTo(new[] { AppRoleBinding.Create("reader", "new-readers") }));
        });
    }

    [Test]
    public async Task InstallAsync_with_an_expected_revision_and_no_record_is_rejected()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);

        var result = await registry.InstallAsync(AppRegistryTestData.Request() with { ExpectedRevision = 1 });

        Assert.That(result.Error, Is.EqualTo(AppRegistryTransitionError.ConcurrencyConflict));
        Assert.That(store.Peek(DefaultKey), Is.Null);
    }
}
