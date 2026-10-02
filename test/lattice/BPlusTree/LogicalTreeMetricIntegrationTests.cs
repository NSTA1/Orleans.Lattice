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

    /// <summary>
    /// The storage-usage gauges must carry the <i>logical</i> tree id even for a
    /// tree whose data now lives in a resized physical copy.
    /// <para>
    /// The WAL poll fans out to every <i>registered</i> tree id, and a resize
    /// registers its <c>{treeId}/resized/{operationId}</c> copy as a tree in its
    /// own right. That copy's WAL-only aggregator therefore samples the very
    /// same WAL partitions the logical tree's aggregator samples and publishes
    /// them a second time, so before the fix the sink held two series for one
    /// tree - one keyed by the logical id and one by the physical copy's - which
    /// both leaks the physical id into the <c>tree</c> label and contradicts the
    /// poller's documented guarantee that a cross-silo <c>sum by (tree)</c>
    /// counts each tree once.
    /// </para>
    /// <para>
    /// The sibling <see cref="Resize_twice_keeps_metric_tree_tags_on_the_logical_tree"/>
    /// case can only observe this when a background poll tick happens to land
    /// inside its window, which is what made it intermittent (issue #4152). This
    /// case drives <see cref="ILatticeAdmin.PollWalUsageAsync"/> directly, so the
    /// observation is a statement about behaviour rather than about timing.
    /// </para>
    /// </summary>
    [Test]
    public async Task Wal_usage_poll_keeps_storage_gauge_tree_tags_on_the_logical_tree()
    {
        var treeId = $"metric-poll-{Guid.NewGuid():N}";
        var measurements = new ConcurrentQueue<(string Instrument, string Tree)>();
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
                if (tag.Key == LatticeMetrics.TagTree && tag.Value is string id
                    && id.StartsWith(treeId, StringComparison.Ordinal))
                    measurements.Enqueue((instrument.Name, id));
        }
        listener.SetMeasurementEventCallback<long>((instrument, _, tags, _) => Capture(instrument, tags));
        listener.SetMeasurementEventCallback<int>((instrument, _, tags, _) => Capture(instrument, tags));
        listener.SetMeasurementEventCallback<double>((instrument, _, tags, _) => Capture(instrument, tags));
        listener.Start();

        var tree = Cluster.GrainFactory.GetGrain<ILattice>(treeId);
        await tree.SetAsync("seed", new byte[] { 1 });
        var resize = Cluster.GrainFactory.GetGrain<ITreeResizeGrain>(treeId);
        await resize.ResizeAsync(16, 16);
        await resize.RunResizePassAsync();
        var physical = await Cluster.GrainFactory.GetLatticeRegistry().ResolveAsync(treeId);
        Assert.That(physical, Is.Not.EqualTo(treeId),
            "The resize must have minted a physical copy for this case to mean anything.");

        // Drive the poll rather than waiting out StorageUsagePollInterval, so the
        // physical copy's aggregator is certain to have published by the time the
        // gauges are observed.
        await Cluster.GrainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey)
            .PollWalUsageAsync(CancellationToken.None);
        listener.RecordObservableInstruments();

        Assert.That(measurements, Is.Not.Empty, "The WAL poll must publish a storage gauge for the tree.");
        Assert.That(measurements.Select(item => item.Tree).Distinct(), Is.EqualTo(new[] { treeId }),
            string.Join(Environment.NewLine, measurements.Where(item => item.Tree != treeId).Distinct()));
    }
}
