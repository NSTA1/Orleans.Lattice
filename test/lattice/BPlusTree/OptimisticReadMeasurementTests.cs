using System.Collections.Concurrent;
using System.Diagnostics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>Matched two-silo point-read measurements with and without continuous point writes.</summary>
[TestFixture]
[Category("Integration")]
[Category("Performance")]
[NonParallelizable]
public sealed class OptimisticReadMeasurementTests
{
    private SmallLeafClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public Task TearDown() => _fixture.DisposeAsync();

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [Explicit("Local paired baseline/candidate measurement, not a timing-sensitive CI gate.")]
    public async Task Measure_point_reads(bool writers, bool absent)
    {
        Assert.That(_fixture.Cluster.Silos, Has.Count.EqualTo(2));
        var treeId = $"optimistic-measure-{writers}-{absent}";
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 1024 });
        var tree = _fixture.Cluster.Client.GetGrain<ILattice>(treeId);
        var value = new byte[128];
        for (var i = 0; i < 8; i++)
            await tree.SetAsync($"key-{i}", value);
        using (var warmup = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
        {
            await Task.WhenAll(Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
            {
                while (!warmup.IsCancellationRequested)
                    await tree.GetAsync(absent ? $"missing-{i}" : $"key-{i}");
            })));
        }

        var outcomes = new ConcurrentDictionary<string, long>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ShardRootOptimisticReadOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, count, tags, _) =>
            {
                string? taggedTree = null;
                string? outcome = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) taggedTree = tag.Value as string;
                    if (tag.Key == LatticeMetrics.TagOutcome) outcome = tag.Value as string;
                }
                if (taggedTree == treeId && outcome is not null)
                    outcomes.AddOrUpdate(outcome, count, (_, total) => total + count);
            }));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        long reads = 0;
        long writes = 0;
        var timer = Stopwatch.StartNew();
        var writeTasks = Enumerable.Range(0, writers ? 4 : 0).Select(i => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                await tree.SetAsync($"key-{i}", value);
                Interlocked.Increment(ref writes);
            }
        })).ToArray();
        var readTasks = Enumerable.Range(0, 8).Select(i => Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var result = await tree.GetAsync(absent ? $"missing-{i}" : $"key-{i}");
                Assert.That(result is null, Is.EqualTo(absent));
                Interlocked.Increment(ref reads);
            }
        })).ToArray();
        await Task.WhenAll(readTasks.Concat(writeTasks)).WaitAsync(TimeSpan.FromMinutes(1));
        timer.Stop();
        var total = outcomes.Values.Sum();
        Assert.That(total, Is.GreaterThan(0));
        TestContext.Out.WriteLine(
            $"MEASURE writers={writers} absent={absent} reads={reads} writes={writes} seconds={timer.Elapsed.TotalSeconds:F3} " +
            $"readsPerSecond={reads / timer.Elapsed.TotalSeconds:F1} validatedPercent={100d * outcomes.GetValueOrDefault("validated") / total:F2} " +
            $"outcomes={string.Join(",", outcomes.OrderBy(p => p.Key).Select(p => $"{p.Key}:{p.Value}"))}");
    }
}
