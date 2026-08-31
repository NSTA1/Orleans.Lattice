using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Backfill;

namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// Covers the small seams the crawl hangs off: the keyed key-source lookup, the
/// default activator's argument contract, and the start-up service that starts
/// each index's crawl.
/// </summary>
/// <remarks>
/// The activator's real work - addressing a grain and having Orleans activate it
/// - cannot be observed without a runtime, so it is proved by the integration
/// tests rather than mimed here.
/// </remarks>
[TestFixture]
public sealed class GrainIndexBackfillSeamTests
{
    private static GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState> Definition() =>
        new(
            "users",
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [new TypedGrainIndexProperty<TestGrainState, int>("Age", static s => s.Age)]);

    [Test]
    public void A_key_source_registered_for_an_index_is_resolved_by_name()
    {
        var source = new ListGrainKeySource(["a"]);
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IGrainKeySource>("users", source);
        using var provider = services.BuildServiceProvider();

        Assert.That(new GrainKeySourceResolver(provider).Resolve("users"), Is.SameAs(source));
    }

    [Test]
    public void An_index_with_no_registered_key_source_resolves_to_null()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();

        Assert.That(new GrainKeySourceResolver(provider).Resolve("users"), Is.Null,
            "Declaring an index without a key source is supported: the activation path still "
            + "indexes every grain that is used.");
    }

    [Test]
    public void A_key_source_registered_for_a_different_index_is_not_resolved()
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IGrainKeySource>("users", new ListGrainKeySource(["a"]));
        using var provider = services.BuildServiceProvider();

        Assert.That(new GrainKeySourceResolver(provider).Resolve("orders"), Is.Null);
    }

    [Test]
    public void The_resolver_rejects_a_null_index_name_and_a_null_container()
    {
        using var provider = new ServiceCollection().BuildServiceProvider();

        Assert.Multiple(() =>
        {
            Assert.That(() => new GrainKeySourceResolver(null!), Throws.ArgumentNullException);
            Assert.That(() => new GrainKeySourceResolver(provider).Resolve(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void The_default_activator_rejects_null_arguments_and_honours_cancellation()
    {
        var activator = new GrainIndexBackfillActivator(Substitute.For<IGrainFactory>());
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(() => new GrainIndexBackfillActivator(null!), Throws.ArgumentNullException);
            Assert.That(
                async () => await activator.ActivateAsync(null!, "a", CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await activator.ActivateAsync(Definition(), null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await activator.ActivateAsync(Definition(), "a", cancelled.Token),
                Throws.InstanceOf<OperationCanceledException>());
        });
    }

    [Test]
    public async Task The_start_up_service_starts_every_index_that_has_a_key_source()
    {
        var harness = new BackfillHarness().WithKeys("a");
        harness.Options.BackfillEnabled = true;

        var backfillGrain = Substitute.For<IGrainIndexBackfillGrain>();
        backfillGrain.EnsureStartedAsync()
            .Returns(Task.FromResult(GrainIndexBackfillStatus.NotStarted(BackfillHarness.IndexName)));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IGrainIndexBackfillGrain>(BackfillHarness.IndexName, Arg.Any<string?>())
            .Returns(backfillGrain);

        await CreateService(harness, factory).StartAsync(CancellationToken.None);

        await backfillGrain.Received(1).EnsureStartedAsync();
    }

    [Test]
    public async Task The_start_up_service_skips_an_index_with_no_key_source()
    {
        var harness = new BackfillHarness();
        harness.Options.BackfillEnabled = true;
        var factory = Substitute.For<IGrainFactory>();

        await CreateService(harness, factory).StartAsync(CancellationToken.None);

        factory.DidNotReceive().GetGrain<IGrainIndexBackfillGrain>(Arg.Any<string>(), Arg.Any<string?>());
    }

    [Test]
    public async Task The_start_up_service_skips_an_index_whose_background_driver_is_off()
    {
        var harness = new BackfillHarness().WithKeys("a");
        harness.Options.BackfillEnabled = false;
        var factory = Substitute.For<IGrainFactory>();

        await CreateService(harness, factory).StartAsync(CancellationToken.None);

        factory.DidNotReceive().GetGrain<IGrainIndexBackfillGrain>(Arg.Any<string>(), Arg.Any<string?>());
    }

    [Test]
    public async Task The_start_up_service_does_not_fail_the_silo_when_a_crawl_cannot_be_started()
    {
        var harness = new BackfillHarness().WithKeys("a");
        harness.Options.BackfillEnabled = true;

        var backfillGrain = Substitute.For<IGrainIndexBackfillGrain>();
        backfillGrain.EnsureStartedAsync()
            .Returns(Task.FromException<GrainIndexBackfillStatus>(
                new InvalidOperationException("the registry was unavailable")));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IGrainIndexBackfillGrain>(BackfillHarness.IndexName, Arg.Any<string?>())
            .Returns(backfillGrain);

        var service = CreateService(harness, factory);

        Assert.That(async () => await service.StartAsync(CancellationToken.None), Throws.Nothing,
            "A backfill that cannot start is a slower index, not a broken silo.");

        await service.StopAsync(CancellationToken.None);
    }

    [Test]
    public void The_start_up_service_rejects_a_null_dependency()
    {
        var harness = new BackfillHarness();
        var declarations = Microsoft.Extensions.Options.Options.Create(new GrainIndexDeclarationOptions());
        var factory = Substitute.For<IGrainFactory>();
        var logger = NullLogger<GrainIndexBackfillHostedService>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexBackfillHostedService(
                    null!, harness.OptionsMonitor, harness.KeySources, factory, logger),
                Throws.ArgumentNullException);

            Assert.That(
                () => new GrainIndexBackfillHostedService(
                    declarations, null!, harness.KeySources, factory, logger),
                Throws.ArgumentNullException);

            Assert.That(
                () => new GrainIndexBackfillHostedService(
                    declarations, harness.OptionsMonitor, null!, factory, logger),
                Throws.ArgumentNullException);

            Assert.That(
                () => new GrainIndexBackfillHostedService(
                    declarations, harness.OptionsMonitor, harness.KeySources, null!, logger),
                Throws.ArgumentNullException);

            Assert.That(
                () => new GrainIndexBackfillHostedService(
                    declarations, harness.OptionsMonitor, harness.KeySources, factory, null!),
                Throws.ArgumentNullException);
        });
    }

    private static GrainIndexBackfillHostedService CreateService(
        BackfillHarness harness,
        IGrainFactory factory)
    {
        var declarations = new GrainIndexDeclarationOptions();
        declarations.Definitions.Add(harness.Definition);

        return new GrainIndexBackfillHostedService(
            Microsoft.Extensions.Options.Options.Create(declarations),
            harness.OptionsMonitor,
            harness.KeySources,
            factory,
            NullLogger<GrainIndexBackfillHostedService>.Instance);
    }
}
