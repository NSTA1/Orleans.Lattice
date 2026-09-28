using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Unit tests for the epic #3807 additions to <see cref="LatticeAppsControl"/>: installing from a named source,
/// recording and updating bridge consent, and describing presentation, UI and source key.
/// </summary>
[TestFixture]
public sealed class LatticeAppsControlSourceAndBridgeTests
{
    private static readonly AppUiBridgeGrantDescriptor Read = new() { Operation = AppUiBridgeOperations.DataRead };

    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    private static AppManifest UiManifest(string version = AppsControlHarness.Version, params AppUiBridgeDeclaration[] bridge) =>
        UiTestManifests.WithUi(AppsControlHarness.Manifest(version), bridge);

    private LatticeAppsControl ControlOver(params IAppCatalogSource[] sources) =>
        new(_h.Registry, new AppSourceSet(sources), _h.Pipeline, _h.Gate, _h.Tenants);

    [Test]
    public async Task InstallAsync_resolves_from_the_named_source_and_records_it_in_provenance()
    {
        var alpha = new TestCatalogSource("alpha").Publish(AppsControlHarness.Manifest());
        var beta = new TestCatalogSource("beta").Publish(AppsControlHarness.Manifest());
        AppRegistryInstallRequest? captured = null;
        _h.Registry.InstallAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        await ControlOver(alpha, beta).InstallAsync(AppsControlHarness.InstallRequest() with { SourceKey = "beta" });

        Assert.That(captured!.Identity.Provenance.Source, Is.EqualTo("beta"));
        Assert.That(alpha.Resolutions, Is.Zero);
        Assert.That(beta.Resolutions, Is.EqualTo(1));
    }

    [Test]
    public void InstallAsync_without_a_source_key_refuses_a_slug_two_sources_offer()
    {
        var control = ControlOver(
            new TestCatalogSource("alpha").Publish(AppsControlHarness.Manifest()),
            new TestCatalogSource("beta").Publish(AppsControlHarness.Manifest()));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => control.InstallAsync(AppsControlHarness.InstallRequest()));

        Assert.That(ex!.Message, Does.Contain("alpha").And.Contain("beta"));
        Assert.That(_h.Registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public void InstallAsync_from_an_unknown_source_is_not_found()
    {
        var control = ControlOver(new TestCatalogSource("alpha").Publish(AppsControlHarness.Manifest()));

        Assert.ThrowsAsync<KeyNotFoundException>(() => control.InstallAsync(AppsControlHarness.InstallRequest() with { SourceKey = "gamma" }));
    }

    [Test]
    public async Task InstallAsync_without_a_source_key_keeps_its_behaviour_for_a_single_source()
    {
        _h.SourceResolves();
        var captured = (AppRegistryInstallRequest?)null;
        _h.Registry.InstallAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        await _h.Control.InstallAsync(AppsControlHarness.InstallRequest());

        Assert.That(captured!.Identity.Provenance.Source, Is.EqualTo("in-image"));
        Assert.That(captured.BridgeConsent, Is.EqualTo(AppUiBridgeRequest.Empty), "a manifest without a UI requests and records no grants");
    }

    [Test]
    public async Task InstallAsync_of_a_fresh_app_consents_to_the_bridge_grants_its_manifest_requests()
    {
        _h.SourceResolves(UiManifest(bridge: [UiTestManifests.Bridge(AppUiBridgeOperations.DataRead, "contacts"), UiTestManifests.Bridge(AppUiBridgeOperations.UiNotify)]));
        AppRegistryInstallRequest? captured = null;
        _h.Registry.InstallAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        await _h.Control.InstallAsync(AppsControlHarness.InstallRequest());

        Assert.That(captured!.BridgeConsent!.Grants, Is.EqualTo(new[]
        {
            new AppUiBridgeGrant(AppUiBridgeOperations.DataRead, "contacts"),
            new AppUiBridgeGrant(AppUiBridgeOperations.UiNotify),
        }));
    }

    [Test]
    public async Task InstallAsync_of_an_upgrade_keeps_the_consented_bridge_grants()
    {
        _h.SourceResolves(UiManifest(AppsControlHarness.OtherVersion, UiTestManifests.Bridge(AppUiBridgeOperations.DataWrite)));
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed));
        AppRegistryInstallRequest? captured = null;
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed, AppsControlHarness.OtherVersion)));

        await _h.Control.InstallAsync(AppsControlHarness.InstallRequest(AppsControlHarness.OtherVersion));

        Assert.That(captured!.BridgeConsent, Is.Null, "an upgrade never widens bridge consent implicitly");
    }

    [Test]
    public void InstallAsync_of_an_enabled_upgrade_that_adds_a_bridge_operation_reports_the_re_consent_it_needs()
    {
        _h.SourceResolves(UiManifest(AppsControlHarness.OtherVersion, UiTestManifests.Bridge(AppUiBridgeOperations.DataWrite)));
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));
        _h.Registry.UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled, AppsControlHarness.OtherVersion)));
        _h.Pipeline.ReconcileAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Reconcile, AppRegistryLifecycleState.Enabled, AppActivationFailure.BridgeConsentRequired));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.InstallAsync(AppsControlHarness.InstallRequest(AppsControlHarness.OtherVersion)));

        Assert.That(ex!.Message, Does.Contain(nameof(AppActivationFailure.BridgeConsentRequired)));
    }

    [Test]
    public async Task UpdateConsentAsync_records_the_supplied_bridge_grants()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed));
        AppRegistryInstallRequest? captured = null;
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        await _h.Control.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            Ceiling = AppsControlHarness.WireCeiling(),
            BridgeGrants = [Read, new AppUiBridgeGrantDescriptor { Operation = AppUiBridgeOperations.DataRead, Tree = "contacts" }],
        });

        Assert.That(captured!.BridgeConsent!.Grants, Is.EqualTo(new[] { new AppUiBridgeGrant(AppUiBridgeOperations.DataRead) }), "normalised");
    }

    [Test]
    public async Task UpdateConsentAsync_without_bridge_grants_leaves_them_unchanged()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed));
        AppRegistryInstallRequest? captured = null;
        _h.Registry.UpgradeAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        await _h.Control.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            Ceiling = AppsControlHarness.WireCeiling(),
        });

        Assert.That(captured!.BridgeConsent, Is.Null);
    }

    [Test]
    public void UpdateConsentAsync_rejects_an_unknown_bridge_operation_before_authorization()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed));

        Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            Ceiling = AppsControlHarness.WireCeiling(),
            BridgeGrants = [new AppUiBridgeGrantDescriptor { Operation = "data.exfiltrate" }],
        }));
        Assert.That(_h.Gate.Requests, Is.Empty);
        Assert.That(_h.Registry.ReceivedCalls(), Is.Empty);
    }

    [Test]
    public async Task GetConsentAsync_reports_the_consented_bridge_grants()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled) with
        {
            ConsentedBridge = AppUiBridgeRequest.Create([new AppUiBridgeGrant(AppUiBridgeOperations.NavSync)]),
        });

        var report = await _h.Control.GetConsentAsync(AppsControlHarness.Slug);

        Assert.That(report!.BridgeGrants, Is.EqualTo(new[] { new AppUiBridgeGrantDescriptor { Operation = AppUiBridgeOperations.NavSync } }));
    }

    [Test]
    public async Task GetConsentAsync_reports_null_bridge_grants_for_a_record_that_never_recorded_any()
    {
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Enabled));

        Assert.That((await _h.Control.GetConsentAsync(AppsControlHarness.Slug))!.BridgeGrants, Is.Null);
    }

    [Test]
    public async Task DescribeAsync_fills_presentation_ui_and_source_key()
    {
        _h.SourceResolves(UiManifest(bridge: [UiTestManifests.Bridge(AppUiBridgeOperations.DataRead)]));

        var descriptor = await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.Version);

        Assert.That(descriptor!.SourceKey, Is.EqualTo("in-image"));
        Assert.That(descriptor.Presentation!.Icon, Is.EqualTo(new AppIconDescriptor
        {
            Path = UiTestManifests.IconPath,
            Sha256 = UiTestManifests.Sha256(UiTestManifests.IconBytes),
        }));
        Assert.That(descriptor.Presentation.Description, Is.EqualTo("Line one.\nLine two."));
        Assert.That(descriptor.Ui!.Assets.Select(a => a.Path), Is.EqualTo(new[] { UiTestManifests.EntryPath, UiTestManifests.IconPath, UiTestManifests.ScriptPath }));
        Assert.That(descriptor.Ui.Scripts.Single(), Is.EqualTo(new AppUiScriptDescriptor { Path = UiTestManifests.ScriptPath, Module = true }));
        Assert.That(descriptor.Ui.Bridge.Single(), Is.EqualTo(Read));
        Assert.That(descriptor.Ui.MinProtocol, Is.EqualTo(1));
    }

    [Test]
    public async Task DescribeAsync_of_a_pre_epic_manifest_leaves_presentation_and_ui_null()
    {
        _h.SourceResolves();

        var descriptor = await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.Version);

        Assert.That(descriptor!.Presentation, Is.Null);
        Assert.That(descriptor.Ui, Is.Null);
    }

    [Test]
    public async Task DescribeAsync_resolves_the_installed_version_from_its_own_source()
    {
        var alpha = new TestCatalogSource("alpha").Publish(AppsControlHarness.Manifest());
        var beta = new TestCatalogSource("beta").Publish(AppsControlHarness.Manifest());
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed) with { Provenance = new AppProvenance { Source = "beta" } });

        var descriptor = await ControlOver(alpha, beta).DescribeAsync(AppsControlHarness.Slug);

        Assert.That(descriptor!.SourceKey, Is.EqualTo("beta"));
        Assert.That(alpha.Resolutions, Is.Zero);
    }

    [Test]
    public void DescribeAsync_of_a_slug_two_sources_offer_without_an_install_reports_the_ambiguity()
    {
        var control = ControlOver(
            new TestCatalogSource("alpha").Publish(AppsControlHarness.Manifest()),
            new TestCatalogSource("beta").Publish(AppsControlHarness.Manifest()));

        Assert.ThrowsAsync<InvalidOperationException>(() => control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.Version));
    }
}
