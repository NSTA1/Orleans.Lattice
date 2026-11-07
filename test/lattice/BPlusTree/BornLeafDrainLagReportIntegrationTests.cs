using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #5969 (W11 on #5605): a resize destination's leaves are built by writes, never by
/// replay, so a split sibling born from one is capped at its donor's replay frontier (#5757,
/// #5769) and carries no checkpoint claim (<c>-1</c>) at most partitions until the live advance
/// first moves it. The drain-lag sampler read that <c>-1</c> as lag from the partition's first
/// append, so every freshly built tree held a lagging consumer per born leaf per written
/// partition and sat over the drain-lag threshold. A negative checkpoint is "no claim", and
/// the sampler treats it as unknown.
/// <para>
/// The precondition proves the destination's born leaves registered a population of consumers,
/// so the bound cannot pass on a destination that reported nothing for the sampler to count.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BornLeafDrainLagReportIntegrationTests
{
    private const int RegisteredPopulation = 16;

    private static readonly TimeSpan RegistrationBudget = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan ObservationWindow = TimeSpan.FromSeconds(5);

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        SiloServicesCapture.Reset();
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
        SiloServicesCapture.Reset();
    }

    [Test]
    public async Task A_freshly_built_resize_destination_does_not_count_its_unclaimed_born_leaves_as_lagging_consumers()
    {
        var lagging = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        using var listener = MeterListening.StartForInstrument(LatticeMetrics.MaterialiserLaggingConsumers, l =>
            l.SetMeasurementEventCallback<int>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree && tag.Value is string tree)
                    {
                        lagging.AddOrUpdate(tree, value, (_, prior) => Math.Max(prior, value));
                    }
                }
            }));

        var treeName = $"born-drain-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        for (var i = 0; i < 60; i++)
        {
            await tree.SetAsync($"key-{i:D4}", [(byte)i]);
        }

        var resize = _cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeName);
        await resize.ResizeAsync(4, 4);
        await resize.RunResizePassAsync();
        var destination = await _cluster.GrainFactory
            .GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).ResolveAsync(treeName);
        Assert.That(destination, Is.Not.EqualTo(treeName), "precondition: the resize aliased the tree to its destination");

        // Steady writes through the alias, as the Journeys corrections after a cutover, then a
        // quiet window several drain-lag thresholds long for the sampler to measure the tree.
        for (var round = 0; round < 3; round++)
        {
            for (var i = 0; i < 60; i++)
            {
                await tree.SetAsync($"key-{i:D4}", [(byte)(round + 1)]);
            }
        }

        // Born leaves register as they first flush, so wait for a population, then observe the
        // sampler for several more drain-lag thresholds before judging it. The metric's tree tag
        // is the logical tree id, which a resize destination keeps.
        var deadline = Environment.TickCount64 + (long)RegistrationBudget.TotalMilliseconds;
        var consumers = 0;
        while (Environment.TickCount64 < deadline
            && (consumers = await RegisteredConsumersAsync(destination)) < RegisteredPopulation)
        {
            await Task.Delay(250);
        }

        await Task.Delay(ObservationWindow);
        consumers = Math.Max(consumers, await RegisteredConsumersAsync(destination));
        var measured = lagging.TryGetValue(treeName, out var max) ? max : 0;

        Assert.That(consumers, Is.GreaterThanOrEqualTo(RegisteredPopulation),
            "precondition: the destination's born leaves registered consumers");
        Assert.That(measured, Is.LessThanOrEqualTo(4),
            $"the sampler counted {measured} lagging consumer(s) of {consumers} on a freshly built resize destination: "
            + "born leaves with no checkpoint claim (-1) were read as lagging from the partition's first append (#5969)");
    }

    private static async Task<int> RegisteredConsumersAsync(string physicalTreeId)
    {
        var prefix = ILeafCursorReporter.MaterialiserConsumerIdPrefix + physicalTreeId + "_";
        var registered = await SiloServices.GetRequiredService<IWalCursorRegistry>().SnapshotAsync(physicalTreeId);
        return registered.Count(c => c.ConsumerId.StartsWith(prefix, StringComparison.Ordinal));
    }

    private static IServiceProvider SiloServices =>
        SiloServicesCapture.Captured ?? throw new InvalidOperationException("The silo service provider was not captured.");

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            // A threshold a test can cross in seconds rather than the default 30 s.
            siloBuilder.ConfigureLattice(o => o.WalSaturationMaterialiserLagThreshold = TimeSpan.FromSeconds(2));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<SiloServicesCapture>();
            siloBuilder.Services.AddHostedService(sp => sp.GetRequiredService<SiloServicesCapture>());
        }
    }

    private sealed class SiloServicesCapture(IServiceProvider services) : IHostedService
    {
        public static IServiceProvider? Captured { get; private set; }

        public static void Reset() => Captured = null;

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Captured = services;
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
