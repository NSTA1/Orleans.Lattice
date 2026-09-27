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
        return (new AppActivationPipeline(factory, status), grain, status, factory);
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
    public void Constructor_rejects_null_dependencies()
    {
        Assert.Throws<ArgumentNullException>(() => new AppActivationPipeline(null!, new InMemoryActivationStatusStore()));
        Assert.Throws<ArgumentNullException>(() => new AppActivationPipeline(Substitute.For<IGrainFactory>(), null!));
    }
}
