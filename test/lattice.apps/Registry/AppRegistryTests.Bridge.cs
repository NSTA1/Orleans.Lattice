namespace Orleans.Lattice.Apps.Tests;

public sealed partial class AppRegistryTests
{
    private static readonly AppUiBridgeRequest ReadConsent = AppUiBridgeRequest.Create([new AppUiBridgeGrant(AppUiBridgeOperations.DataRead)]);

    private static readonly AppUiBridgeRequest ReadWriteConsent = AppUiBridgeRequest.Create(
        [new AppUiBridgeGrant(AppUiBridgeOperations.DataRead), new AppUiBridgeGrant(AppUiBridgeOperations.DataWrite)]);

    [Test]
    public async Task Install_records_the_bridge_consent_it_is_given()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        var result = await registry.InstallAsync(AppRegistryTestData.Request() with { BridgeConsent = ReadConsent });

        Assert.That(result.Record!.ConsentedBridge, Is.EqualTo(ReadConsent));
    }

    [Test]
    public async Task Install_without_bridge_consent_records_none()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());

        var result = await registry.InstallAsync(AppRegistryTestData.Request());

        Assert.That(result.Record!.ConsentedBridge, Is.Null);
    }

    [Test]
    public async Task Upgrade_without_bridge_consent_keeps_the_consented_grants()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        await registry.InstallAsync(AppRegistryTestData.Request() with { BridgeConsent = ReadConsent });

        var upgraded = await registry.UpgradeAsync(AppRegistryTestData.Request(AppRegistryTestData.V2));

        Assert.That(upgraded.Succeeded, Is.True, upgraded.Message);
        Assert.That(upgraded.Record!.ConsentedBridge, Is.EqualTo(ReadConsent));
    }

    [Test]
    public async Task Upgrade_with_bridge_consent_replaces_the_consented_grants()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        await registry.InstallAsync(AppRegistryTestData.Request() with { BridgeConsent = ReadConsent });

        var upgraded = await registry.UpgradeAsync(AppRegistryTestData.Request() with { BridgeConsent = ReadWriteConsent });

        Assert.That(upgraded.Record!.ConsentedBridge, Is.EqualTo(ReadWriteConsent));
    }

    [Test]
    public async Task Reinstall_after_uninstall_does_not_inherit_the_previous_bridge_consent()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        await registry.InstallAsync(AppRegistryTestData.Request() with { BridgeConsent = ReadWriteConsent });
        await registry.UninstallAsync(TenantId.Default, AppRegistryTestData.Slug);

        var reinstalled = await registry.InstallAsync(AppRegistryTestData.Request());

        Assert.That(reinstalled.Succeeded, Is.True, reinstalled.Message);
        Assert.That(reinstalled.Record!.ConsentedBridge, Is.Null);
    }

    [Test]
    public async Task Enable_and_disable_carry_the_bridge_consent_unchanged()
    {
        var registry = AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore());
        await registry.InstallAsync(AppRegistryTestData.Request() with { BridgeConsent = ReadConsent });

        var enabled = await registry.EnableAsync(TenantId.Default, AppRegistryTestData.Slug);
        var disabled = await registry.DisableAsync(TenantId.Default, AppRegistryTestData.Slug);

        Assert.That(enabled.Record!.ConsentedBridge, Is.EqualTo(ReadConsent));
        Assert.That(disabled.Record!.ConsentedBridge, Is.EqualTo(ReadConsent));
    }
}
