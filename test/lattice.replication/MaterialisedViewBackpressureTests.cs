using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Integration coverage for the view maintainer obeying the source tree's WAL
/// saturation back-pressure signal. A single-silo cluster overrides
/// <see cref="IWalSaturationSignal"/> with a controllable fake so a test can force
/// the source regime, then drives convergence through the public
/// <see cref="ILatticeView.WaitForSourceHeadAsync"/> barrier and asserts both that
/// the view still converges under back-pressure and that the maintainer recorded
/// the self-throttle on <c>orleans.lattice.view.source_backpressure</c> - and that
/// it does not self-throttle when <see cref="LatticeViewOptions.ObeySourceBackpressure"/>
/// is disabled.
/// </summary>
[TestFixture]
[Category("Integration")]
public class MaterialisedViewBackpressureTests
{
    private static readonly TimeSpan Barrier = TimeSpan.FromSeconds(20);

    private const string ObeyViewName = "bp-adults";
    private const string ObeySourceTreeId = "bp-people";
    private const string NoObeyViewName = "nobp-adults";
    private const string NoObeySourceTreeId = "nobp-people";

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices =>
        _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    private sealed record Person(int Age);

    private static byte[] PersonBytes(int age) =>
        JsonLatticeSerializer<Person>.Default.Serialize(new Person(age));

    private static PredicateLatticeViewProjection AdultFilter() =>
        new(LatticePredicateTranslator.Translate<Person>(p => p.Age >= 18));

    [Test]
    public async Task Maintainer_self_throttles_and_still_converges_when_source_is_saturated()
    {
        var signal = SiloServices.GetRequiredService<ControllableWalSaturationSignal>();
        signal.State = WalSaturationState.Saturated;

        using var backpressure = new MeterCollector<long>(
            LatticeMetrics.MeterName, "orleans.lattice.view.source_backpressure");

        var source = _cluster.Client.GetGrain<ILattice>(ObeySourceTreeId);
        // More than the saturated drip-feed batch (16) so convergence spans several
        // throttled passes rather than a single full drain.
        for (var age = 18; age < 58; age++)
        {
            await source.SetAsync($"p{age}", PersonBytes(age));
        }

        var view = SiloServices.GetRequiredService<ILatticeViewFactory>()
            .Create(source, ObeyViewName, new LatticeViewDefinition(ObeyViewName, AdultFilter()));

        await view.WaitForSourceHeadAsync(Barrier);

        var throttledForThisView = backpressure.Measurements
            .Where(m => m.Tags.Any(t => t.Key == LatticeMetrics.TagView && (string?)t.Value == ObeyViewName))
            .ToArray();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await view.CountAsync(), Is.EqualTo(40),
                "the view must still converge to every in-predicate source key under source back-pressure");
            Assert.That(throttledForThisView, Is.Not.Empty,
                "the maintainer must record a self-throttle while the source is saturated");
            Assert.That(
                throttledForThisView.All(m => m.Tags.Any(t =>
                    t.Key == LatticeMetrics.TagWalSaturationState && (string?)t.Value == "saturated")),
                Is.True,
                "every recorded self-throttle should carry the observed saturated regime tag");
        });
    }

    [Test]
    public async Task Maintainer_drains_full_rate_when_obey_source_backpressure_is_disabled()
    {
        var signal = SiloServices.GetRequiredService<ControllableWalSaturationSignal>();
        signal.State = WalSaturationState.Saturated;

        using var backpressure = new MeterCollector<long>(
            LatticeMetrics.MeterName, "orleans.lattice.view.source_backpressure");

        var source = _cluster.Client.GetGrain<ILattice>(NoObeySourceTreeId);
        for (var age = 18; age < 38; age++)
        {
            await source.SetAsync($"p{age}", PersonBytes(age));
        }

        var view = SiloServices.GetRequiredService<ILatticeViewFactory>()
            .Create(source, NoObeyViewName, new LatticeViewDefinition(NoObeyViewName, AdultFilter()));

        await view.WaitForSourceHeadAsync(Barrier);

        var throttledForThisView = backpressure.Measurements
            .Where(m => m.Tags.Any(t => t.Key == LatticeMetrics.TagView && (string?)t.Value == NoObeyViewName))
            .ToArray();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await view.CountAsync(), Is.EqualTo(20),
                "the view must converge regardless of the obey switch");
            Assert.That(throttledForThisView, Is.Empty,
                "a view with ObeySourceBackpressure disabled must not self-throttle even when the source is saturated");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews();

            // Override the per-silo saturation signal with a controllable fake so a
            // test can force the source regime. Registered after AddLattice so this
            // (non-TryAdd) registration is the one resolved by the maintainer grain.
            siloBuilder.Services.AddSingleton<ControllableWalSaturationSignal>();
            siloBuilder.Services.AddSingleton<IWalSaturationSignal>(
                sp => sp.GetRequiredService<ControllableWalSaturationSignal>());

            // Keep the background drain timer dormant; convergence is driven through
            // the public WaitForSourceHeadAsync barrier (foreground drains, which
            // still apply the back-pressure batch scaling).
            siloBuilder.Services.ConfigureAll<LatticeViewOptions>(o =>
                o.CoalesceWindow = TimeSpan.FromMinutes(5));

            siloBuilder.Services.Configure<LatticeViewOptions>(
                NoObeyViewName, o => o.ObeySourceBackpressure = false);
        }
    }
}

/// <summary>
/// Test double for <see cref="IWalSaturationSignal"/> whose reported regime is set
/// directly by a test. <see cref="WaitForHealthyAsync"/> never blocks so a forced
/// saturated regime cannot deadlock the source write path.
/// </summary>
internal sealed class ControllableWalSaturationSignal : IWalSaturationSignal
{
    public volatile WalSaturationState State = WalSaturationState.Healthy;

    public WalSaturationState GetCurrentState(string treeId) => State;

    public WalSaturationState GetAggregateState() => State;

    public Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;
}
