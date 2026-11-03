using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Re-triage of issue #5612: a runtime change-history view created over a busy,
/// multi-shard, full-value-retention source before its seed, then a seed, a
/// migration-style rewrite of every key and a continuous feed, with the real
/// drain timer, keepalive reminder and WAL GC all running. The view must keep
/// up, its maintainer must keep answering a queued (non-reentrant) call within
/// a bounded time, neither the source's nor the view's WAL floor may wedge, and
/// an unrelated tree must stay readable throughout.
/// </summary>
[TestFixture]
[Category("Integration")]
public class HistoryViewBusySourceTests
{
    private const string SourceTreeId = "hist-busy-src";
    private const string BystanderTreeId = "hist-busy-bystander";
    private const string ViewName = "hist-busy-view";
    private const int KeyCount = 240;
    private const int RewriteRounds = 3;
    private const int Writers = 8;

    private static readonly TimeSpan QueuedCallBound = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan BystanderReadBound = TimeSpan.FromSeconds(10);

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices
        => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task A_history_view_over_a_busy_source_keeps_up_without_stalling_its_maintainer_or_wedging_the_wal()
    {
        var client = _cluster.Client;
        var registry = client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(
            SourceTreeId,
            new TreeRegistryEntry { ShardCount = 4, HistoryRetentionMode = HistoryRetentionMode.FullValue });
        var source = client.GetGrain<ILattice>(SourceTreeId);
        var bystander = client.GetGrain<ILattice>(BystanderTreeId);
        await bystander.SetAsync("probe", [1]);

        // Created before the seed, as U4 created orders-history over factory-orders.
        await SiloServices.GetRequiredService<ILatticeViewFactory>()
            .CreateAsync(source, ViewName, LatticeHistoryView.Definition(ViewName, SiloServices));
        var maintainer = client.GetGrain<IViewMaintainerGrain>(ViewName);
        await maintainer.EnsureActiveAsync();
        var generation = await maintainer.GetActiveTreeIdAsync();

        // Seed, then rewrite every key RewriteRounds times (a migration and a feed),
        // while a sampler times a queued maintainer call and a bystander read.
        using var writing = new CancellationTokenSource();
        var sampler = SampleAsync(maintainer, bystander, writing.Token);
        var writes = 0;
        for (var round = 0; round <= RewriteRounds; round++)
        {
            var r = round;
            await Parallel.ForEachAsync(
                Enumerable.Range(0, KeyCount),
                new ParallelOptions { MaxDegreeOfParallelism = Writers },
                async (i, _) => await source.SetAsync(Key(i), Value(i, r)));
            writes += KeyCount;
        }

        writing.Cancel();
        var (maxQueuedCall, maxBystanderRead, samples) = await sampler;

        await maintainer.WaitForSourceHeadAsync(TimeSpan.FromSeconds(60));
        var rows = await CountHistoryRowsAsync(maintainer);
        var sourceProbe = await ProbeAsync(SourceTreeId);
        var viewProbe = await ProbeAsync(await maintainer.GetActiveTreeIdAsync());

        Assert.Multiple(async () =>
        {
            Assert.That(samples, Is.GreaterThan(0), "the sampler measured the maintainer while the source was written");
            Assert.That(maxQueuedCall, Is.LessThan(QueuedCallBound),
                "a queued maintainer call is answered within a bound while the source is busy");
            Assert.That(maxBystanderRead, Is.LessThan(BystanderReadBound),
                "an unrelated tree stays readable while the view drains");
            Assert.That(await maintainer.GetLagAsync(), Is.Zero, "the view caught up to the source head");
            Assert.That(await maintainer.GetActiveTreeIdAsync(), Is.EqualTo(generation),
                "the view kept up by tailing, never falling off the log into a rebuild");
            Assert.That(rows, Is.EqualTo(writes), "the view recorded one revision per mutation");
            Assert.That(sourceProbe.PinStoreReadable, Is.True, "the source's pin store answered");
            Assert.That(sourceProbe.IsWedged, Is.False, $"the source's WAL floor is not wedged: {sourceProbe}");
            Assert.That(viewProbe.PinStoreReadable, Is.True, "the view tree's pin store answered");
            Assert.That(viewProbe.IsWedged, Is.False, $"the view tree's WAL floor is not wedged: {viewProbe}");
        });
    }

    private static string Key(int i) => $"order/{i:D6}";

    private static byte[] Value(int i, int round) =>
        Encoding.UTF8.GetBytes(
            $"{{\"id\":{i},\"round\":{round},\"status\":\"{(round % 2 == 0 ? "open" : "shipped")}\",\"total\":{(i * 7) + round},\"notes\":\"{new string('x', 160)}\"}}");

    private static async Task<(TimeSpan MaxQueuedCall, TimeSpan MaxBystanderRead, int Samples)> SampleAsync(
        IViewMaintainerGrain maintainer,
        ILattice bystander,
        CancellationToken stop)
    {
        var maxQueued = TimeSpan.Zero;
        var maxRead = TimeSpan.Zero;
        var samples = 0;
        while (!stop.IsCancellationRequested)
        {
            // DrainAsync is not interleaved, so its latency includes any turn the
            // maintainer is stuck in (the #5612 symptom was a keepalive turn
            // active for minutes with requests queued behind it).
            var watch = Stopwatch.StartNew();
            await maintainer.DrainAsync();
            maxQueued = watch.Elapsed > maxQueued ? watch.Elapsed : maxQueued;

            watch.Restart();
            await bystander.GetAsync("probe");
            maxRead = watch.Elapsed > maxRead ? watch.Elapsed : maxRead;
            samples++;
        }

        return (maxQueued, maxRead, samples);
    }

    private Task<WalFloorHolderProbeReport> ProbeAsync(string physicalTreeId) =>
        _cluster.Client.GetGrain<ILatticeWalFloorHolderProbe>(physicalTreeId).ProbeAsync(CancellationToken.None);

    private async Task<int> CountHistoryRowsAsync(IViewMaintainerGrain maintainer)
    {
        var viewTree = _cluster.Client.GetGrain<ILattice>(await maintainer.GetActiveTreeIdAsync());
        var rows = 0;
        using var scope = ViewReadContext.BeginScope();
        await foreach (var _ in viewTree.ScanEntriesAsync())
        {
            rows++;
        }

        return rows;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeWalGc();
            // The real GC runs often, so a blocked floor would show while the view drains.
            siloBuilder.ConfigureLattice(o => o.WalGcInterval = TimeSpan.FromSeconds(1));
            siloBuilder.AddLatticeViews();
        }
    }
}
