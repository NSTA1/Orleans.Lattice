using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Regression tests for issue #4021: a fresh install consents to the bridge grants of the manifest
/// resolved again at commit, so without a pin a dynamic source could add a non-data bridge operation
/// between the operator's review and the install. The install request now carries the reviewed
/// <see cref="AppDescriptor.ManifestDigest"/>, and the commit refuses on a mismatch.
/// </summary>
[TestFixture]
public sealed class LatticeAppsControlManifestPinTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp()
    {
        _h = new AppsControlHarness();
        _h.Registry.InstallAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));
    }

    private static AppManifest Reviewed() =>
        UiTestManifests.WithUi(AppsControlHarness.Manifest(), UiTestManifests.Bridge(AppUiBridgeOperations.DataRead, "contacts"));

    private static AppManifest Changed() =>
        UiTestManifests.WithUi(
            AppsControlHarness.Manifest(),
            UiTestManifests.Bridge(AppUiBridgeOperations.DataRead, "contacts"),
            UiTestManifests.Bridge(AppUiBridgeOperations.ContextUser));

    /// <summary>A source that serves <paramref name="first"/> to the review, then <paramref name="then"/> to every later read.</summary>
    private void SourceServes(AppManifest first, AppManifest then)
    {
        var provenance = new AppProvenance { Source = "in-image", Publisher = "contoso", Reference = "asm" };
        var calls = 0;
        _h.Source.ResolveAsync(Arg.Any<AppSlug>(), Arg.Any<AppVersion?>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                var manifest = Interlocked.Increment(ref calls) == 1 ? first : then;
                var handle = Substitute.For<IAppActivationHandle>();
                handle.Identity.Returns(manifest.Identity);
                return new ValueTask<AppSourceResult>(AppSourceResult.Resolved(manifest, provenance, handle));
            });
    }

    [Test]
    public async Task InstallAsync_pinned_to_the_reviewed_digest_refuses_a_manifest_that_changed_since_review()
    {
        SourceServes(Reviewed(), Changed());
        var reviewed = await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.Version);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.InstallAsync(
            AppsControlHarness.InstallRequest() with { ExpectedManifestDigest = reviewed!.ManifestDigest }));

        Assert.That(ex!.Message, Does.Contain("reviewed"));
        await _h.Registry.DidNotReceive().InstallAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>());
        await _h.Registry.DidNotReceive().UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InstallAsync_pinned_to_the_reviewed_digest_installs_an_unchanged_manifest()
    {
        SourceServes(Reviewed(), Reviewed());
        var reviewed = await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.Version);
        AppRegistryInstallRequest? captured = null;
        _h.Registry.InstallAsync(Arg.Do<AppRegistryInstallRequest>(r => captured = r), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Succeeded(AppsControlHarness.Record(AppRegistryLifecycleState.Installed)));

        await _h.Control.InstallAsync(AppsControlHarness.InstallRequest() with { ExpectedManifestDigest = reviewed!.ManifestDigest });

        Assert.That(captured!.BridgeConsent!.Grants, Is.EqualTo(new[] { new AppUiBridgeGrant(AppUiBridgeOperations.DataRead, "contacts") }));
    }

    [Test]
    public async Task InstallAsync_pinned_to_the_reviewed_digest_refuses_an_upgrade_whose_manifest_changed()
    {
        var reviewed = UiTestManifests.WithUi(AppsControlHarness.Manifest(AppsControlHarness.OtherVersion));
        var changed = UiTestManifests.WithUi(
            AppsControlHarness.Manifest(AppsControlHarness.OtherVersion), UiTestManifests.Bridge(AppUiBridgeOperations.ContextUser));
        SourceServes(reviewed, changed);
        var described = await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.OtherVersion);
        _h.RegistryHas(AppsControlHarness.Record(AppRegistryLifecycleState.Installed));

        Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.InstallAsync(
            AppsControlHarness.InstallRequest(AppsControlHarness.OtherVersion) with { ExpectedManifestDigest = described!.ManifestDigest }));
        await _h.Registry.DidNotReceive().UpgradeAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task InstallAsync_without_a_pin_keeps_the_behaviour_of_a_server_that_predates_it()
    {
        SourceServes(Reviewed(), Changed());
        _ = await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.Version);

        await _h.Control.InstallAsync(AppsControlHarness.InstallRequest());

        await _h.Registry.Received(1).InstallAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>());
    }

    [TestCase("")]
    [TestCase("not-a-digest")]
    [TestCase("ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789")]
    public void InstallAsync_rejects_a_malformed_pin_before_authorization(string digest)
    {
        _h.SourceResolves();

        Assert.ThrowsAsync<ArgumentException>(() => _h.Control.InstallAsync(
            AppsControlHarness.InstallRequest() with { ExpectedManifestDigest = digest }));
        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }

    [Test]
    public async Task DescribeAsync_reports_a_digest_that_follows_the_manifest_content()
    {
        SourceServes(Reviewed(), Changed());

        var first = await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.Version);
        var second = await _h.Control.DescribeAsync(AppsControlHarness.Slug, AppsControlHarness.Version);

        Assert.That(first!.ManifestDigest, Does.Match("^[0-9a-f]{64}$"));
        Assert.That(second!.ManifestDigest, Is.Not.EqualTo(first.ManifestDigest));
    }

    [Test]
    public async Task DescribeFromSourceAsync_reports_the_digest_the_install_pins_against()
    {
        var source = new TestCatalogSource("alpha").Publish(Reviewed());
        var catalog = new CatalogHarness().With(source).Catalog;
        var control = new LatticeAppsControl(_h.Registry, new AppSourceSet([source]), _h.Pipeline, _h.Gate, _h.Tenants);

        var described = await catalog.DescribeFromSourceAsync("alpha", AppsControlHarness.Slug, AppsControlHarness.Version);
        await control.InstallAsync(AppsControlHarness.InstallRequest() with
        {
            SourceKey = "alpha",
            ExpectedManifestDigest = described!.ManifestDigest,
        });

        await _h.Registry.Received(1).InstallAsync(Arg.Any<AppRegistryInstallRequest>(), Arg.Any<CancellationToken>());
    }
}
