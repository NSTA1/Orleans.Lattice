using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class LatticeAppsControlConsentTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    private static AppConsentUpdate Update(string version = AppsControlHarness.Version, params AppExceptionScope[] scopes) =>
        new() { Slug = AppsControlHarness.Slug, Version = version, Ceiling = AppsControlHarness.WireCeiling(scopes) };

    [Test]
    public async Task GetConsentAsync_returns_pinned_ceiling_with_app_local_scopes()
    {
        var ceiling = new AppCapabilityCeiling
        {
            AllowedOperations = LatticeOperation.Read | LatticeOperation.Write,
            ApprovedExceptionScopes =
            [
                LatticeScope.Prefix("a/billing/ledger", "inv/"),
                LatticeScope.Tree("legacy-contacts"),
            ],
        };
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled, ceiling: ceiling));

        var report = await _h.Control.GetConsentAsync(AppsControlHarness.Slug);

        Assert.That(report, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(report!.Slug, Is.EqualTo(AppsControlHarness.Slug));
            Assert.That(report.Version, Is.EqualTo(AppsControlHarness.Version));
            Assert.That(report.Ceiling.AllowedOperations, Is.EqualTo(LatticeOperation.Read | LatticeOperation.Write));
            Assert.That(report.Ceiling.ApprovedExceptionScopes, Is.EqualTo(new[]
            {
                new AppExceptionScope { Kind = LatticeScopeKind.Prefix, App = "billing", Tree = "ledger", KeyOrPrefix = "inv/" },
                new AppExceptionScope { AdoptedTreeId = "legacy-contacts" },
            }));
        });
    }

    [Test]
    public async Task GetConsentAsync_absent_or_uninstalled_app_returns_null()
    {
        _h.RegistryHas(null);
        Assert.That(await _h.Control.GetConsentAsync(AppsControlHarness.Slug), Is.Null);

        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Uninstalled));
        Assert.That(await _h.Control.GetConsentAsync(AppsControlHarness.Slug), Is.Null);
    }

    [Test]
    public async Task UpdateConsentAsync_replaces_ceiling_keeping_identity_bindings_and_state()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Disabled);
        _h.RegistryHas(current);
        AppRegistryInstallRequest? captured = null;
        var updated = current with { Ceiling = AppCapabilityCeiling.Structural(LatticeOperation.Read) };
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(updated));

        var report = await _h.Control.UpdateConsentAsync(Update());

        Assert.That(report.Ceiling.AllowedOperations, Is.EqualTo(LatticeOperation.Read));
        Assert.Multiple(() =>
        {
            Assert.That(captured!.Identity.Version, Is.EqualTo(current.Version));
            Assert.That(captured.Identity.Provenance, Is.EqualTo(current.Provenance));
            Assert.That(captured.RoleBindings, Is.EqualTo(current.RoleBindings));
            Assert.That(captured.Ceiling.AllowedOperations, Is.EqualTo(LatticeOperation.Read));
        });
        await _h.Registry.DidNotReceiveWithAnyArgs().EnableAsync(default, default, default);
        await _h.Pipeline.DidNotReceiveWithAnyArgs().ReconcileAsync(default, default, default);
    }

    [Test]
    public async Task UpdateConsentAsync_on_enabled_app_reconciles_grants()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Enabled);
        _h.RegistryHas(current);
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(current));
        _h.Pipeline.ReconcileAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled));

        await _h.Control.UpdateConsentAsync(Update());

        await _h.Pipeline.Received(1).ReconcileAsync(TenantId.Default, AppsControlHarness.AppSlugValue, Arg.Any<CancellationToken>());
    }

    [Test]
    public void UpdateConsentAsync_reconcile_failure_throws_noting_recorded_consent()
    {
        var current = AppsControlHarness.Record(AppRegistryLifecycleState.Enabled);
        _h.RegistryHas(current);
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(current));
        _h.Pipeline.ReconcileAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled, AppActivationFailure.CeilingExceeded));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateConsentAsync(Update()));
        Assert.That(ex!.Message, Does.StartWith("The consent was recorded"));
    }

    [Test]
    public void UpdateConsentAsync_version_mismatch_fails_without_mutation()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));

        Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateConsentAsync(Update(AppsControlHarness.OtherVersion)));
        _ = _h.Registry.DidNotReceiveWithAnyArgs().UpgradeAsync(default!, default);
    }

    [Test]
    public void UpdateConsentAsync_not_installed_throws_key_not_found()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Uninstalled));

        Assert.ThrowsAsync<KeyNotFoundException>(() => _h.Control.UpdateConsentAsync(Update()));
    }

    [Test]
    public void UpdateConsentAsync_rejected_upgrade_throws_invalid_operation()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed));
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Rejected(AppRegistryTransitionError.ConcurrencyConflict, "Changed concurrently."));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.UpdateConsentAsync(Update()));
        Assert.That(ex!.Message, Does.Contain("ConcurrencyConflict").And.Contain("Changed concurrently"));
    }

    [Test]
    public void UpdateConsentAsync_rejects_invalid_requests_before_authorization()
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentNullException>(() => _h.Control.UpdateConsentAsync(null!));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateConsentAsync(Update() with { Slug = "x" }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateConsentAsync(Update() with { Version = "latest" }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateConsentAsync(Update() with { Ceiling = null! }));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateConsentAsync(
                Update(scopes: new AppExceptionScope { App = "billing", Tree = "ledger", AdoptedTreeId = "legacy" })));
        });
        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }
}
