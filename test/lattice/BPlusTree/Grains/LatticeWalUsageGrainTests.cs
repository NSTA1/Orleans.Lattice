using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LatticeWalUsageGrain"/>, the leaf-free
/// WAL-only storage-usage aggregator. The headline regression these tests
/// pin is that the aggregator the cluster-wide poller targets touches only
/// WAL partition grains - never a leaf, internal node, snapshot storage,
/// or shard-root grain - so an idle "cold" tree is never activated by
/// polling.
/// </summary>
[TestFixture]
public sealed class LatticeWalUsageGrainTests
{
    private const string TreeId = "wal-usage-tree";

    private static LatticeWalUsageGrain CreateGrain(
        IGrainFactory factory,
        out LatticeStorageUsageMetrics metrics,
        LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("ol.gwu", TreeId));

        // The aggregator now resolves the physical tree id directly through
        // the registry instead of going via ILattice.GetRoutingAsync, so
        // the user-facing grain (which sits in the producer's hot path)
        // is never activated by polling. TestOptionsResolver.ForFactory
        // already wires the registry to return an entry with
        // PhysicalTreeId=null, which the SUT correctly resolves to TreeId.

        var resolver = TestOptionsResolver.ForFactory(factory, options);
        metrics = new LatticeStorageUsageMetrics();
        return new LatticeWalUsageGrain(
            context,
            factory,
            resolver,
            metrics,
            Substitute.For<ILogger<LatticeWalUsageGrain>>());
    }

    [Test]
    public async Task GetWalUsageAsync_only_touches_wal_grains_never_leaves_or_snapshots()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(1234L);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);

        var grain = CreateGrain(factory, out _);

        var report = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(TreeId));
            Assert.That(report.WalRetainedBytes, Is.EqualTo(1234L));
            Assert.That(report.Partial, Is.False);
        });

        // Headline cold-tree regression: the aggregator must not activate any
        // shard root, leaf, internal node, snapshot storage grain, or the
        // public ILattice surface (the producer's hot path). If a future
        // change re-introduces a leaf-walk in the polling path, these
        // assertions fail before the cold-tree regression can escape
        // into CI.
        factory.DidNotReceiveWithAnyArgs().GetGrain<IShardRootGrain>(default!);
        factory.DidNotReceiveWithAnyArgs().GetGrain<IBPlusLeafGrain>(default(Guid));
        factory.DidNotReceiveWithAnyArgs().GetGrain<IBPlusInternalGrain>(default(Guid));
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILeafSnapshotStorageGrain>(default(Guid));
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILattice>(default!);
    }

    [Test]
    public async Task GetWalUsageAsync_unsupported_provider_sets_partial()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(-1L);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);

        var grain = CreateGrain(factory, out _);

        var report = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(report.Partial, Is.True);
            Assert.That(report.WalRetainedBytes, Is.EqualTo(0L),
                "an unsupported partition contributes zero, not the -1 sentinel");
        });
    }

    [Test]
    public async Task GetWalUsageAsync_publishes_wal_bytes_to_metrics_sink()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(500L);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);

        var grain = CreateGrain(factory, out var metrics);

        await grain.GetWalUsageAsync(CancellationToken.None);

        // Read the WAL bytes back through the sink to prove the publish
        // landed.
        Assert.That(ReadWalGauge(TreeId), Is.EqualTo(500L));
    }

    [Test]
    public async Task GetWalUsageAsync_drives_over_threshold_when_policy_configured()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(800L);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);

        var grain = CreateGrain(factory, out _, options: new LatticeOptions { WalMaxRetainedBytes = 500L });

        await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.That(ReadOverThresholdGauge(TreeId), Is.EqualTo(1L),
            "800 retained bytes against a 500-byte ceiling must report over-threshold = 1");
    }

    [Test]
    public async Task GetWalUsageAsync_leaves_over_threshold_unset_without_policy()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(800L);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);

        var grain = CreateGrain(factory, out _, options: new LatticeOptions { WalMaxRetainedBytes = null });

        await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.That(ReadOverThresholdGauge(TreeId), Is.Null,
            "no advisory ceiling configured = no over-threshold measurement");
    }

    [Test]
    public async Task GetWalUsageAsync_serves_cached_report_within_ttl_without_refanning_out()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(1234L);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);

        // Large TTL so a sequential second call is well inside the window.
        var grain = CreateGrain(factory, out _, options: new LatticeOptions { StorageUsageCacheTtl = TimeSpan.FromMinutes(5) });

        var first = await grain.GetWalUsageAsync(CancellationToken.None);
        var second = await grain.GetWalUsageAsync(CancellationToken.None);

        Assert.That(first.SampledAt, Is.EqualTo(second.SampledAt),
            "the second call within the cache TTL must serve the same sampled report");
        // The WAL provider must have been queried exactly once across both
        // calls; the second call is served from the per-activation cache
        // and must not pile a duplicate Azure Table query onto the same
        // connection pool the foreground writes share.
        await wal.Received(1).GetRetainedByteSizeAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task GetWalUsageAsync_coalesces_concurrent_callers_behind_one_in_flight_fan_out()
    {
        var factory = Substitute.For<IGrainFactory>();
        var wal = Substitute.For<IWalShardGrain>();
        var gate = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        wal.GetRetainedByteSizeAsync(Arg.Any<CancellationToken>()).Returns(_ => gate.Task);
        factory.GetGrain<IWalShardGrain>($"{TreeId}/0").Returns(wal);

        var grain = CreateGrain(factory, out _);

        // Two concurrent callers arrive while the fan-out is in flight.
        var callerA = grain.GetWalUsageAsync(CancellationToken.None);
        var callerB = grain.GetWalUsageAsync(CancellationToken.None);

        // Release the gate so both callers complete from the SAME
        // underlying fan-out.
        gate.SetResult(999L);
        var resultA = await callerA;
        var resultB = await callerB;

        Assert.Multiple(() =>
        {
            Assert.That(resultA.WalRetainedBytes, Is.EqualTo(999L));
            Assert.That(resultB.WalRetainedBytes, Is.EqualTo(999L));
            Assert.That(resultA.SampledAt, Is.EqualTo(resultB.SampledAt),
                "both callers must observe the same sampled report from a single in-flight fan-out");
        });
        await wal.Received(1).GetRetainedByteSizeAsync(Arg.Any<CancellationToken>());
    }

    private static long? ReadWalGauge(string treeId)
        => ReadGauge(LatticeMetrics.StorageWalBytesName, treeId);

    private static long? ReadOverThresholdGauge(string treeId)
        => ReadGauge(LatticeMetrics.StoragePolicyOverThresholdName, treeId);

    private static long? ReadGauge(string instrument, string tree)
    {
        long? found = null;
        using var listener = new System.Diagnostics.Metrics.MeterListener
        {
            InstrumentPublished = (inst, l) =>
            {
                if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter) && inst.Name == instrument)
                    l.EnableMeasurementEvents(inst);
            },
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var t in tags)
            {
                if (t.Key == LatticeMetrics.TagTree && (string?)t.Value == tree)
                {
                    found = value;
                }
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return found;
    }
}
