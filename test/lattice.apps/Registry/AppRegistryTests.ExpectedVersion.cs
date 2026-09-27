namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// <see cref="AppRegistryInstallRequest.ExpectedVersion"/>: an upgrade decided against a read
/// of the installed version is applied only while that version is still installed, so a
/// consent update or upgrade racing another upgrade cannot silently roll it back.
/// </summary>
public sealed partial class AppRegistryTests
{
    [Test]
    public async Task UpgradeAsync_with_a_stale_expected_version_is_rejected_and_changes_nothing()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        await registry.InstallAsync(AppRegistryTestData.Request());
        var upgraded = await registry.UpgradeAsync(AppRegistryTestData.Request(version: AppRegistryTestData.V2));
        Assert.That(upgraded.Succeeded, Is.True);

        // A consent update that read V1 before the upgrade landed must not write V1 back.
        var stale = await registry.UpgradeAsync(AppRegistryTestData.Request() with { ExpectedVersion = AppRegistryTestData.V1 });

        Assert.That(stale.Succeeded, Is.False);
        Assert.That(stale.Error, Is.EqualTo(AppRegistryTransitionError.ConcurrencyConflict));
        Assert.That(stale.Changed, Is.False);
        Assert.That(store.Peek(DefaultKey)!.Version, Is.EqualTo(AppRegistryTestData.V2));
    }

    [Test]
    public async Task UpgradeAsync_with_the_current_expected_version_applies()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);
        await registry.InstallAsync(AppRegistryTestData.Request());

        var result = await registry.UpgradeAsync(
            AppRegistryTestData.Request(version: AppRegistryTestData.V2) with { ExpectedVersion = AppRegistryTestData.V1 });

        Assert.That(result.Succeeded, Is.True);
        Assert.That(store.Peek(DefaultKey)!.Version, Is.EqualTo(AppRegistryTestData.V2));
    }

    [Test]
    public async Task InstallAsync_ignores_no_expected_version_and_rejects_a_mismatched_one()
    {
        var store = new InMemoryAppRegistryStore();
        var registry = AppRegistryTestData.CreateRegistry(store);

        var mismatched = await registry.InstallAsync(AppRegistryTestData.Request() with { ExpectedVersion = AppRegistryTestData.V2 });
        Assert.That(mismatched.Error, Is.EqualTo(AppRegistryTransitionError.ConcurrencyConflict));
        Assert.That(store.Peek(DefaultKey), Is.Null);

        var installed = await registry.InstallAsync(AppRegistryTestData.Request());
        Assert.That(installed.Succeeded, Is.True);
    }
}
