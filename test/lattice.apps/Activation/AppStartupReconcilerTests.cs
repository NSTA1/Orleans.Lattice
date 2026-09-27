using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppStartupReconciler"/>: the background reconcile of enabled apps
/// never affects silo startup, whatever the apps' manifests look like.
/// </summary>
[TestFixture]
public sealed class AppStartupReconcilerTests
{
    private static readonly AppSlug Good = AppSlug.Parse("good-app");
    private static readonly AppSlug Invalid = AppSlug.Parse("invalid-app");
    private static readonly AppSlug OverCeiling = AppSlug.Parse("greedy-app");

    private static AppStartupReconciler Create(
        IAppRegistry registry,
        IAppActivationPipeline pipeline,
        LatticeAppsOptions? options = null) =>
        new(
            registry,
            pipeline,
            new StaticOptionsMonitor(options ?? new LatticeAppsOptions()),
            NullLogger<AppStartupReconciler>.Instance);

    private static async Task<ActivationHarness> SeedEnabledAppsAsync()
    {
        var harness = new ActivationHarness();
        var bindings = new[] { AppRoleBinding.Create("reader", "readers") };

        // A good app, an app whose manifest will be invalid, and an over-ceiling app, all enabled.
        foreach (var slug in new[] { Good, Invalid, OverCeiling })
        {
            await harness.InstallAsync(ActivationHarness.Manifest(slug: slug), bindings: bindings);
            var enabled = await harness.RunAsync(AppActivationOperation.Enable, slug: slug);
            Assert.That(enabled.Succeeded, Is.True);
        }

        harness.Source.Fail(AppSourceResult.InvalidManifest(Invalid, new[] { new AppManifestError("json", "$", "not json") }));
        await harness.UpgradeAsync(
            ActivationHarness.Manifest(slug: OverCeiling, version: ActivationHarness.V2,
                roles: new[] { ActivationHarness.Role("reader", LatticeOperation.Read | LatticeOperation.Delete, "records") }),
            ceiling: AppCapabilityCeiling.Structural(LatticeOperation.Read),
            bindings: bindings);
        return harness;
    }

    [Test]
    public async Task Invalid_and_over_ceiling_manifests_leave_startup_unaffected()
    {
        var harness = await SeedEnabledAppsAsync();
        var reconciler = Create(harness.Registry, new EngineBackedPipeline(harness.Engine, harness.Status));

        // StartAsync returns without waiting on activation, and the background work completes
        // without faulting - the host is never stopped by a broken app.
        await reconciler.StartAsync(CancellationToken.None);
        await reconciler.ExecuteTask!;

        Assert.That(reconciler.ExecuteTask.IsCompletedSuccessfully, Is.True);
        var good = await harness.Status.GetAsync(TenantId.Default, Good, CancellationToken.None);
        var invalid = await harness.Status.GetAsync(TenantId.Default, Invalid, CancellationToken.None);
        var greedy = await harness.Status.GetAsync(TenantId.Default, OverCeiling, CancellationToken.None);
        Assert.That(good!.LastOutcome.Operation, Is.EqualTo(AppActivationOperation.Reconcile));
        Assert.That(good.LastOutcome.Succeeded, Is.True);
        Assert.That(invalid!.LastOutcome.Failure, Is.EqualTo(AppActivationFailure.InvalidManifest));
        Assert.That(greedy!.LastOutcome.Failure, Is.EqualTo(AppActivationFailure.CeilingExceeded));
        await reconciler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ReconcileEnabledAppsAsync_reconciles_only_enabled_apps()
    {
        var harness = await SeedEnabledAppsAsync();
        await harness.RunAsync(AppActivationOperation.Disable, slug: Good);
        var reconciler = Create(harness.Registry, new EngineBackedPipeline(harness.Engine, harness.Status));

        var outcomes = await reconciler.ReconcileEnabledAppsAsync(CancellationToken.None);

        Assert.That(outcomes.Select(o => o.Slug), Is.EquivalentTo(new[] { Invalid, OverCeiling }));
    }

    [Test]
    public async Task A_pipeline_exception_for_one_app_does_not_stop_the_others()
    {
        var harness = await SeedEnabledAppsAsync();
        var pipeline = new EngineBackedPipeline(harness.Engine, harness.Status)
        {
            Throw = slug => slug == Invalid ? new TimeoutException("grain call timed out") : null,
        };
        var reconciler = Create(harness.Registry, pipeline);

        var outcomes = await reconciler.ReconcileEnabledAppsAsync(CancellationToken.None);

        Assert.That(outcomes.Select(o => o.Slug), Is.EquivalentTo(new[] { Good, OverCeiling }));
    }

    [Test]
    public async Task The_registry_read_is_retried_until_it_succeeds()
    {
        var harness = await SeedEnabledAppsAsync();
        var registry = Substitute.For<IAppRegistry>();
        var calls = 0;
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => ++calls == 1
            ? throw new InvalidOperationException("silo not ready")
            : harness.Registry.ListAsync());
        var reconciler = Create(registry, new EngineBackedPipeline(harness.Engine, harness.Status), new LatticeAppsOptions
        {
            StartupRetryDelay = TimeSpan.FromMilliseconds(1),
            StartupRetryMaxDelay = TimeSpan.FromMilliseconds(1),
        });

        var outcomes = await reconciler.ReconcileEnabledAppsAsync(CancellationToken.None);

        Assert.That(calls, Is.EqualTo(2));
        Assert.That(outcomes, Has.Count.EqualTo(3));
    }

    [Test]
    public async Task Startup_reconcile_can_be_turned_off()
    {
        var registry = Substitute.For<IAppRegistry>();
        var reconciler = Create(registry, Substitute.For<IAppActivationPipeline>(), new LatticeAppsOptions { ReconcileOnStartup = false });

        await reconciler.StartAsync(CancellationToken.None);
        await reconciler.ExecuteTask!;

        registry.DidNotReceiveWithAnyArgs().ListAsync(default);
    }

    [Test]
    public async Task A_registry_that_never_becomes_readable_stops_cleanly_with_the_host()
    {
        var registry = Substitute.For<IAppRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => throw new InvalidOperationException("silo not ready"));
        var reconciler = Create(registry, Substitute.For<IAppActivationPipeline>(), new LatticeAppsOptions
        {
            StartupRetryDelay = TimeSpan.FromMilliseconds(1),
            StartupRetryMaxDelay = TimeSpan.FromMilliseconds(5),
        });

        await reconciler.StartAsync(CancellationToken.None);
        await reconciler.StopAsync(CancellationToken.None);

        Assert.That(reconciler.ExecuteTask!.IsCompleted, Is.True);
        Assert.That(reconciler.ExecuteTask.IsFaulted, Is.False);
    }

    private sealed class StaticOptionsMonitor(LatticeAppsOptions value) : IOptionsMonitor<LatticeAppsOptions>
    {
        public LatticeAppsOptions CurrentValue => value;

        public LatticeAppsOptions Get(string? name) => value;

        public IDisposable? OnChange(Action<LatticeAppsOptions, string?> listener) => null;
    }
}
