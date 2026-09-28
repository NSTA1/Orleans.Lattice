using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
[Category("Integration")]
public class LogicalTreeMetricIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster Cluster => _fixture.Cluster;

    [OneTimeSetUp]
    public async Task SetUpAsync()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDownAsync() => await _fixture.DisposeAsync();

    [TestCase(false)]
    [TestCase(true)]
    public async Task Resize_twice_keeps_metric_tree_tags_on_the_logical_tree(bool tenantScoped)
    {
        using var origin = tenantScoped ? LatticeAccessGateContext.EnterSystemOrigin() : null;
        var treeId = $"{(tenantScoped ? "t/metric-tenant/" : "")}metric-alias-{Guid.NewGuid():N}";
        var measurements = new ConcurrentQueue<(string Instrument, string Tree)>();
        var tenants = new ConcurrentQueue<string?>();
        using var listener = new MeterListener();
        var meter = LatticeMetrics.Meter;
        listener.InstrumentPublished = (instrument, current) =>
        {
            if (ReferenceEquals(instrument.Meter, meter))
                current.EnableMeasurementEvents(instrument);
        };
        void Capture(Instrument instrument, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            foreach (var tag in tags)
                if (tag.Key == LatticeMetrics.TagTree && tag.Value is string id && id.StartsWith(treeId, StringComparison.Ordinal))
                {
                    measurements.Enqueue((instrument.Name, id));
                    foreach (var dimension in tags)
                        if (dimension.Key == "tenant")
                            tenants.Enqueue(dimension.Value as string);
                }
        }
        listener.SetMeasurementEventCallback<long>((instrument, _, tags, _) => Capture(instrument, tags));
        listener.SetMeasurementEventCallback<int>((instrument, _, tags, _) => Capture(instrument, tags));
        listener.SetMeasurementEventCallback<double>((instrument, _, tags, _) => Capture(instrument, tags));
        listener.Start();
        var tree = Cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("seed", new byte[] { 1 });
        for (var generation = 0; generation < 2; generation++)
        {
            var resize = Cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
            await resize.ResizeAsync(16 + generation * 16, 16 + generation * 16);
            await resize.RunResizePassAsync();
            var physical = await Cluster.GrainFactory.GetLatticeRegistry().ResolveAsync(treeId);
            Assert.That(physical, Is.Not.EqualTo(treeId));
            var beforeWrites = measurements.Count;
            await tree.SetAsync($"after-{generation}", new byte[] { 2 });
            await tree.SetManyAtomicAsync(new() { new($"atomic-{generation}", new byte[] { 3 }) });
            Assert.That(await tree.GetAsync($"after-{generation}"), Is.Not.Null);
            Assert.That(await tree.GetAsync($"after-{generation}"), Is.Not.Null);
            await tree.GetManyAsync([$"after-{generation}", $"missing-{generation}"]);
            await Cluster.GrainFactory.GetGrain<IShardRootGrain>($"{physical}/0")
                .GetShardProjectionDigestAsync(CancellationToken.None);
            foreach (var instrument in new[] { LatticeMetrics.ShardWrites.Name, LatticeMetrics.LeafWriteDuration.Name,
                         LatticeMetrics.WalAppendBatchEntries.Name, LatticeMetrics.AtomicWriteCompleted.Name,
                         LatticeMetrics.CacheMisses.Name })
                Assert.That(measurements.Skip(beforeWrites).Any(item => item.Instrument == instrument),
                    Is.True, $"Generation {generation}: {instrument}");
        }
        listener.RecordObservableInstruments();
        Assert.That(measurements, Is.Not.Empty);
        Assert.That(tenants, Is.Not.Empty);
        Assert.That(tenants.Distinct(), Is.EqualTo(new[] { LatticeTenantLabel.Resolve(treeId) }));
        Assert.That(measurements.Select(item => item.Tree).Distinct(), Is.EqualTo(new[] { treeId }),
            string.Join(Environment.NewLine, measurements.Where(item => item.Tree != treeId).Distinct()));
    }
}
