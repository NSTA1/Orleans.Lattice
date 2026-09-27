using NSubstitute;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// The facade decides every upgrade against a read of the installed version, so it passes that
/// version to the registry as <see cref="AppRegistryInstallRequest.ExpectedVersion"/>: a consent
/// update or upgrade that loses a race with another upgrade is refused instead of rolling it back.
/// </summary>
[TestFixture]
public sealed class LatticeAppsControlUpgradeRaceTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    [Test]
    public async Task UpdateConsentAsync_pins_the_upgrade_to_the_version_it_read()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Disabled);
        _h.RegistryHas(current);
        AppRegistryInstallRequest? captured = null;
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(current));

        await _h.Control.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            Ceiling = AppsControlHarness.WireCeiling(),
        });

        Assert.That(captured!.ExpectedVersion, Is.EqualTo(AppsControlHarness.V(AppsControlHarness.Version)));
    }

    [Test]
    public async Task InstallAsync_upgrade_pins_the_upgrade_to_the_version_it_read()
    {
        _h.SourceResolves(AppsControlHarness.Manifest(AppsControlHarness.OtherVersion));
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Disabled);
        _h.RegistryHas(current);
        AppRegistryInstallRequest? captured = null;
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Disabled, AppsControlHarness.OtherVersion)));

        await _h.Control.InstallAsync(AppsControlHarness.InstallRequest(AppsControlHarness.OtherVersion));

        Assert.That(captured!.ExpectedVersion, Is.EqualTo(AppsControlHarness.V(AppsControlHarness.Version)));
        Assert.That(captured.Identity.Version, Is.EqualTo(AppsControlHarness.V(AppsControlHarness.OtherVersion)));
    }

    [Test]
    public async Task UpdateConsentAsync_surfaces_a_lost_race_as_a_failed_precondition()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Rejected(AppRegistryTransitionError.ConcurrencyConflict));

        Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            Ceiling = AppsControlHarness.WireCeiling(),
        }));
        await _h.Pipeline.DidNotReceiveWithAnyArgs().ReconcileAsync(default, default, default);
    }
}
