using NSubstitute;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Unit tests for <see cref="AppActivationPipeline"/>: it validates arguments and routes each run
/// to the app's serializing activation grain, keyed <c>{tenant}/{slug}</c>.
/// </summary>
[TestFixture]
public sealed class AppActivationPipelineTests
{
    private static readonly string Key = AppRegistryTreeNames.ComposeKey(TenantId.Default, ActivationHarness.Slug);

    private static (AppActivationPipeline Pipeline, IAppActivationGrain Grain, InMemoryActivationStatusStore Status, IGrainFactory Factory) Create()
    {
        var grain = Substitute.For<IAppActivationGrain>();
        grain.ExecuteAsync(default, default, default, default)
            .ReturnsForAnyArgs(call => Task.FromResult(new AppActivationOutcome
            {
                Tenant = call.ArgAt<TenantId>(1),
                Slug = call.ArgAt<AppSlug>(2),
                Operation = call.ArgAt<AppActivationOperation>(0),
            }));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IAppActivationGrain>(Key, null).Returns(grain);
        var status = new InMemoryActivationStatusStore();
        return (new AppActivationPipeline(factory, status, AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore()), NullAppSource.Instance), grain, status, factory);
    }

    [TestCase(AppActivationOperation.Enable)]
    [TestCase(AppActivationOperation.Disable)]
    [TestCase(AppActivationOperation.Uninstall)]
    [TestCase(AppActivationOperation.Reconcile)]
    public async Task Each_verb_routes_to_the_app_grain(AppActivationOperation operation)
    {
        var (pipeline, grain, _, _) = Create();

        var outcome = operation switch
        {
            AppActivationOperation.Enable => await pipeline.EnableAsync(TenantId.Default, ActivationHarness.Slug),
            AppActivationOperation.Disable => await pipeline.DisableAsync(TenantId.Default, ActivationHarness.Slug),
            AppActivationOperation.Uninstall => await pipeline.UninstallAsync(TenantId.Default, ActivationHarness.Slug),
            _ => await pipeline.ReconcileAsync(TenantId.Default, ActivationHarness.Slug),
        };

        Assert.That(outcome.Operation, Is.EqualTo(operation));
        await grain.Received(1).ExecuteAsync(operation, TenantId.Default, ActivationHarness.Slug, Arg.Any<CancellationToken>());
    }

    [Test]
    public void Uninitialised_arguments_are_rejected_before_any_grain_call()
    {
        var (pipeline, _, _, factory) = Create();

        Assert.Throws<ArgumentException>(() => pipeline.EnableAsync(default, ActivationHarness.Slug));
        Assert.Throws<ArgumentException>(() => pipeline.DisableAsync(TenantId.Default, default));
        Assert.Throws<ArgumentException>(() => pipeline.GetStatusAsync(TenantId.Default, default));
        factory.DidNotReceiveWithAnyArgs().GetGrain<IAppActivationGrain>(default(string)!, default);
    }

    [Test]
    public async Task GetStatusAsync_reads_the_recorded_status()
    {
        var (pipeline, _, status, _) = Create();
        Assert.That(await pipeline.GetStatusAsync(TenantId.Default, ActivationHarness.Slug), Is.Null);

        var recorded = new AppActivationStatus
        {
            Tenant = TenantId.Default,
            Slug = ActivationHarness.Slug,
            LastOutcome = new AppActivationOutcome { Tenant = TenantId.Default, Slug = ActivationHarness.Slug, Operation = AppActivationOperation.Enable },
        };
        await status.SetAsync(recorded, CancellationToken.None);

        Assert.That(await pipeline.GetStatusAsync(TenantId.Default, ActivationHarness.Slug), Is.EqualTo(recorded));
    }

    [Test]
    public async Task UninstallAsync_reconciles_only_the_enabled_dependants_of_the_uninstalled_app()
    {
        var store = new InMemoryAppRegistryStore();
        var source = new ActivationAppSource();
        var factory = Substitute.For<IGrainFactory>();
        var grains = new Dictionary<string, IAppActivationGrain>(StringComparer.Ordinal);
        factory.GetGrain<IAppActivationGrain>(Arg.Any<string>(), null).Returns(call =>
        {
            var key = call.ArgAt<string>(0);
            if (!grains.TryGetValue(key, out var grain))
            {
                grain = Substitute.For<IAppActivationGrain>();
                grain.ExecuteAsync(default, default, default, default).ReturnsForAnyArgs(c => Task.FromResult(new AppActivationOutcome
                {
                    Tenant = c.ArgAt<TenantId>(1),
                    Slug = c.ArgAt<AppSlug>(2),
                    Operation = c.ArgAt<AppActivationOperation>(0),
                }));
                grains[key] = grain;
            }

            return grain;
        });

        void Seed(string slug, AppRegistryLifecycleState state, bool dependsOnNotes, bool publish = true)
        {
            var app = AppSlug.Parse(slug);
            store.Seed(AppRegistryTreeNames.ComposeKey(TenantId.Default, app), AppRegistryTestData.Record(state, slug: app));
            if (publish)
            {
                source.Publish(ActivationHarness.Manifest(slug: app) with
                {
                    Subscriptions = dependsOnNotes
                        ? [new AppSubscriptionDeclaration { Name = "feed", Tree = "records", App = ActivationHarness.Slug }]
                        : [],
                });
            }
        }

        Seed("crm", AppRegistryLifecycleState.Enabled, dependsOnNotes: true);
        Seed("hr", AppRegistryLifecycleState.Enabled, dependsOnNotes: false);
        Seed("old", AppRegistryLifecycleState.Disabled, dependsOnNotes: true);
        Seed("lost", AppRegistryLifecycleState.Enabled, dependsOnNotes: false, publish: false);
        var pipeline = new AppActivationPipeline(factory, new InMemoryActivationStatusStore(), AppRegistryTestData.CreateRegistry(store), source);

        var outcome = await pipeline.UninstallAsync(TenantId.Default, ActivationHarness.Slug);

        Assert.That(outcome.Succeeded, Is.True);
        var reconciled = grains
            .Where(pair => pair.Value.ReceivedCalls().Any(c => (AppActivationOperation)c.GetArguments()[0]! == AppActivationOperation.Reconcile))
            .Select(pair => pair.Key)
            .ToArray();
        Assert.That(reconciled, Is.EquivalentTo(new[] { "default/crm", "default/lost" }),
            "an enabled dependant, and one whose manifest cannot be resolved, are reconciled; others are not");
    }

    [Test]
    public async Task UninstallAsync_reconciles_a_dependant_declared_through_a_cross_app_role_scope()
    {
        // The existing fan-out test declares its dependency as a subscription. A
        // cross-app ROLE SCOPE is the other way an app reaches into the uninstalled
        // one, and it is the one that carries a live grant: missing it would leave a
        // dependant holding authorization over trees whose owner no longer exists.
        var store = new InMemoryAppRegistryStore();
        var source = new ActivationAppSource();
        var grains = new Dictionary<string, IAppActivationGrain>(StringComparer.Ordinal);
        var factory = GrainFactoryRecording(grains);

        void Seed(string slug, AppScopeTemplate[] scopes)
        {
            var app = AppSlug.Parse(slug);
            store.Seed(AppRegistryTreeNames.ComposeKey(TenantId.Default, app), AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, slug: app));
            source.Publish(ActivationHarness.Manifest(slug: app) with
            {
                Roles = [new AppRoleDeclaration { Name = "reader", Operations = LatticeOperation.Read, Scopes = scopes }],
                Subscriptions = [],
            });
        }

        // "crm" scopes a tree owned by the app being uninstalled; "hr" scopes only
        // its own, so it must not be reconciled.
        Seed("crm", [new AppScopeTemplate { Tree = "records", App = ActivationHarness.Slug }]);
        Seed("hr", [new AppScopeTemplate { Tree = "records" }]);
        var pipeline = new AppActivationPipeline(factory, new InMemoryActivationStatusStore(), AppRegistryTestData.CreateRegistry(store), source);

        var outcome = await pipeline.UninstallAsync(TenantId.Default, ActivationHarness.Slug);

        Assert.That(outcome.Succeeded, Is.True);
        Assert.That(Reconciled(grains), Is.EquivalentTo(new[] { "default/crm" }),
            "only the app whose role scope names the uninstalled owner is reconciled");
    }

    private static IGrainFactory GrainFactoryRecording(Dictionary<string, IAppActivationGrain> grains)
    {
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IAppActivationGrain>(Arg.Any<string>(), null).Returns(call =>
        {
            var key = call.ArgAt<string>(0);
            if (!grains.TryGetValue(key, out var grain))
            {
                grain = Substitute.For<IAppActivationGrain>();
                grain.ExecuteAsync(default, default, default, default).ReturnsForAnyArgs(c => Task.FromResult(new AppActivationOutcome
                {
                    Tenant = c.ArgAt<TenantId>(1),
                    Slug = c.ArgAt<AppSlug>(2),
                    Operation = c.ArgAt<AppActivationOperation>(0),
                }));
                grains[key] = grain;
            }

            return grain;
        });
        return factory;
    }

    private static string[] Reconciled(Dictionary<string, IAppActivationGrain> grains) => grains
        .Where(pair => pair.Value.ReceivedCalls().Any(c => (AppActivationOperation)c.GetArguments()[0]! == AppActivationOperation.Reconcile))
        .Select(pair => pair.Key)
        .ToArray();

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        Assert.Throws<ArgumentNullException>(() => new AppActivationPipeline(null!, new InMemoryActivationStatusStore(), AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore()), NullAppSource.Instance));
        Assert.Throws<ArgumentNullException>(() => new AppActivationPipeline(Substitute.For<IGrainFactory>(), null!, AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore()), NullAppSource.Instance));
        Assert.Throws<ArgumentNullException>(() => new AppActivationPipeline(Substitute.For<IGrainFactory>(), new InMemoryActivationStatusStore(), null!, NullAppSource.Instance));
        Assert.Throws<ArgumentNullException>(() => new AppActivationPipeline(Substitute.For<IGrainFactory>(), new InMemoryActivationStatusStore(), AppRegistryTestData.CreateRegistry(new InMemoryAppRegistryStore()), null!));
    }
}
