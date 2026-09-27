using NSubstitute;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests;

[TestFixture]
public sealed class LatticeAppsControlLifecycleTests
{
    private AppsControlHarness _h = null!;

    [SetUp]
    public void SetUp() => _h = new AppsControlHarness();

    [Test]
    public void Constructor_null_dependency_throws()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAppsControl(null!, _h.Source, _h.Pipeline, _h.Gate, _h.Tenants));
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAppsControl(_h.Registry, null!, _h.Pipeline, _h.Gate, _h.Tenants));
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAppsControl(_h.Registry, _h.Source, null!, _h.Gate, _h.Tenants));
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAppsControl(_h.Registry, _h.Source, _h.Pipeline, null!, _h.Tenants));
            Assert.Throws<ArgumentNullException>(() => _ = new LatticeAppsControl(_h.Registry, _h.Source, _h.Pipeline, _h.Gate, null!));
        });
    }

    [Test]
    public async Task EnableAsync_delegates_to_pipeline_and_maps_outcome()
    {
        _h.Pipeline.EnableAsync(TenantId.Default, AppsControlHarness.AppSlugValue, Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Enable, AppRegistryLifecycleState.Enabled));

        var result = await _h.Control.EnableAsync(AppsControlHarness.Slug);

        Assert.That(result, Is.EqualTo(new AppLifecycleResult
        {
            Slug = AppsControlHarness.Slug,
            Version = AppsControlHarness.Version,
            State = AppLifecycleState.Enabled,
            Changed = true,
        }));
        Assert.That(_h.Gate.Requests.Single().Operation, Is.EqualTo(LatticeOperation.AppInstall));
    }

    [Test]
    public async Task DisableAsync_delegates_to_pipeline_and_reports_unchanged_repeat()
    {
        _h.Pipeline.DisableAsync(TenantId.Default, AppsControlHarness.AppSlugValue, Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Disable, AppRegistryLifecycleState.Disabled, changed: false));

        var result = await _h.Control.DisableAsync(AppsControlHarness.Slug);

        Assert.That(result.State, Is.EqualTo(AppLifecycleState.Disabled));
        Assert.That(result.Changed, Is.False);
    }

    [Test]
    public async Task UninstallAsync_delegates_to_pipeline()
    {
        _h.Pipeline.UninstallAsync(TenantId.Default, AppsControlHarness.AppSlugValue, Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Uninstall, AppRegistryLifecycleState.Uninstalled));

        var result = await _h.Control.UninstallAsync(AppsControlHarness.Slug);

        Assert.That(result.State, Is.EqualTo(AppLifecycleState.Uninstalled));
        await _h.Registry.DidNotReceiveWithAnyArgs().UninstallAsync(default, default, default);
    }

    [Test]
    public void EnableAsync_not_installed_throws_key_not_found()
    {
        _h.Pipeline.EnableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Enable, null, AppActivationFailure.NotInstalled, version: null));

        var ex = Assert.ThrowsAsync<KeyNotFoundException>(() => _h.Control.EnableAsync(AppsControlHarness.Slug));
        Assert.That(ex!.Message, Does.Contain("'crm'"));
    }

    [TestCase(AppActivationFailure.CeilingExceeded)]
    [TestCase(AppActivationFailure.InvalidTransition)]
    [TestCase(AppActivationFailure.Faulted)]
    public void EnableAsync_failed_activation_throws_invalid_operation(AppActivationFailure failure)
    {
        _h.Pipeline.EnableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Enable, AppRegistryLifecycleState.Installed, failure, diagnostic: "details."));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.EnableAsync(AppsControlHarness.Slug));
        Assert.That(ex!.Message, Does.Contain(failure.ToString()).And.Contain("details"));
    }

    [Test]
    public void EnableAsync_success_without_state_throws_invalid_operation()
    {
        _h.Pipeline.EnableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(AppsControlHarness.Outcome(AppActivationOperation.Enable, null, version: null));

        Assert.ThrowsAsync<InvalidOperationException>(() => _h.Control.EnableAsync(AppsControlHarness.Slug));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("Bad Slug")]
    [TestCase("a/crm/contacts")]
    public void Lifecycle_verbs_reject_invalid_slug_before_authorization(string? slug)
    {
        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.EnableAsync(slug!));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.DisableAsync(slug!));
            Assert.ThrowsAsync<ArgumentException>(() => _h.Control.UninstallAsync(slug!));
        });
        Assert.That(_h.Gate.Requests, Is.Empty);
        _h.AssertEngineUntouched();
    }

    [Test]
    public void Lifecycle_cancellation_stays_cancellation_with_composed_id_stripped()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        _h.Pipeline.EnableAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns<AppActivationOutcome>(_ => throw new OperationCanceledException("Stopped at t/acme/a/crm/contacts.", cts.Token));

        var ex = Assert.CatchAsync<OperationCanceledException>(() => _h.Control.EnableAsync(AppsControlHarness.Slug, cts.Token));
        Assert.That(ex!.Message, Is.EqualTo("Stopped at contacts."));
        Assert.That(ex.CancellationToken, Is.EqualTo(cts.Token));
    }
}
