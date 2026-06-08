using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the writer-side dispatch deadline
/// (<see cref="LatticeOptions.WalAppendDispatchTimeout"/>). The writer's
/// outbound <see cref="IWalShardGrain.AppendAsync"/> /
/// <see cref="IWalShardGrain.AppendBatchAsync"/> RPC was historically
/// unbounded on the writer side; a wedged shard would hold every caller
/// parked until the Orleans response timeout (default 3 minutes). The
/// deadline converts that blind hang into a structured
/// <see cref="TimeoutException"/> with per-shard attribution via the
/// <see cref="LatticeMetrics.WalAppendDispatchTimeouts"/> counter.
/// </summary>
[TestFixture]
public class WalCommitLogWriterDispatchTimeoutTests
{
    private const string TreeId = "tree-dispatch";

    /// <summary>
    /// Captures every measurement reported on the
    /// <see cref="LatticeMetrics.Meter"/> instrument set for the lifetime
    /// of the test, mirroring the same pattern used in
    /// <c>WalShardGrainTests.G023Diagnostics</c>.
    /// </summary>
    private sealed class MeterCapture : IDisposable
    {
        private readonly MeterListener _listener;
        public ConcurrentBag<(string Name, double Value, KeyValuePair<string, object?>[] Tags)> Records { get; } = new();

        public MeterCapture()
        {
            _listener = new MeterListener
            {
                InstrumentPublished = (inst, l) =>
                {
                    if (ReferenceEquals(inst.Meter, LatticeMetrics.Meter))
                    {
                        l.EnableMeasurementEvents(inst);
                    }
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (inst, value, tags, _) => Records.Add((inst.Name, value, tags.ToArray())));
            _listener.Start();
        }

        public long Count(string instrumentName) =>
            Records.Where(r => r.Name == instrumentName).Sum(r => (long)r.Value);

        public (long Value, KeyValuePair<string, object?>[] Tags)? FirstFor(string instrumentName)
        {
            var hit = Records.FirstOrDefault(r => r.Name == instrumentName);
            return hit == default ? null : ((long)hit.Value, hit.Tags);
        }

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Builds a writer whose target <see cref="IWalShardGrain"/> is the
    /// supplied substitute, so the test can install per-shard behaviours
    /// (hang forever, complete promptly, etc.) by configuring the
    /// substitute before exercising the writer.
    /// </summary>
    private static WalCommitLogWriter CreateWriter(
        IWalShardGrain shard,
        LatticeOptions? options = null,
        string clusterId = "site-test")
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns(clusterId);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: optionsMonitor.Get(string.Empty), factory: grainFactory);
        return new WalCommitLogWriter(grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver);
    }

    private static WalRecord MakeMutation(string key = "k") => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-test",
    };

    [Test]
    public async Task AppendAsync_completes_promptly_without_tripping_dispatch_deadline()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(42L));

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromSeconds(5),
        });

        var offset = await writer.AppendAsync(MakeMutation());
        Assert.That(offset, Is.EqualTo(42L));
        Assert.That(capture.Count("orleans.lattice.wal.append_dispatch.timeouts"), Is.EqualTo(0L),
            "happy path must never trip the dispatch-deadline counter");
    }

    [Test]
    public void AppendAsync_throws_TimeoutException_and_increments_counter_when_grain_hangs_past_deadline()
    {
        using var capture = new MeterCapture();
        var release = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(release.Task);

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
        });

        Assert.That(
            async () => await writer.AppendAsync(MakeMutation()),
            Throws.TypeOf<TimeoutException>()
                  .With.Message.Contains(nameof(LatticeOptions.WalAppendDispatchTimeout)));

        Assert.That(capture.Count("orleans.lattice.wal.append_dispatch.timeouts"), Is.EqualTo(1L),
            "deadline trip must increment the dispatch-timeout counter exactly once");

        var sample = capture.FirstFor("orleans.lattice.wal.append_dispatch.timeouts");
        Assert.That(sample, Is.Not.Null);
        Assert.That(
            sample!.Value.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == TreeId),
            Is.True,
            "deadline trip must be tagged with the affected tree id for per-shard attribution");
        Assert.That(
            sample.Value.Tags.Any(t => t.Key == LatticeMetrics.TagShard),
            Is.True,
            "deadline trip must be tagged with the affected shard index for per-shard attribution");

        // Unparking the substitute so the abandoned task can settle and
        // the test runner does not see a leaked never-completing task.
        release.TrySetResult(0L);
    }

    [Test]
    public async Task AppendAsync_with_infinite_dispatch_timeout_preserves_unbounded_await()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(7L));

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = Timeout.InfiniteTimeSpan,
        });

        var offset = await writer.AppendAsync(MakeMutation());
        Assert.That(offset, Is.EqualTo(7L));
        Assert.That(capture.Count("orleans.lattice.wal.append_dispatch.timeouts"), Is.EqualTo(0L),
            "infinite dispatch deadline must never trip the counter");
    }

    [Test]
    public void AppendManyAsync_throws_TimeoutException_and_increments_counter_when_batched_grain_hangs()
    {
        using var capture = new MeterCapture();
        var release = new TaskCompletionSource<IReadOnlyList<long>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
             .Returns(release.Task);

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
        });

        // Two-entry batch routes through the batched AppendForPartitionAsync
        // path rather than the single-entry AppendAsync fast-path
        // (AppendManyAsync short-circuits to AppendAsync when entries.Count == 1).
        // The two entries may land on different WAL partitions; both paths
        // dispatch into the same hanging substitute, so the assertion is
        // "at least one dispatch tripped the deadline" rather than
        // "exactly one" - the per-shard attribution is asserted via the
        // tag presence rather than the count.
        var entries = new[] { MakeMutation("a"), MakeMutation("b") };

        Assert.That(
            async () => await writer.AppendManyAsync(entries),
            Throws.TypeOf<TimeoutException>()
                  .With.Message.Contains(nameof(LatticeOptions.WalAppendDispatchTimeout)));

        Assert.That(capture.Count("orleans.lattice.wal.append_dispatch.timeouts"), Is.GreaterThanOrEqualTo(1L),
            "batched-path deadline trip must increment the dispatch-timeout counter at least once");

        var sample = capture.FirstFor("orleans.lattice.wal.append_dispatch.timeouts");
        Assert.That(sample, Is.Not.Null);
        Assert.That(
            sample!.Value.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == TreeId),
            Is.True,
            "batched-path deadline trip must be tagged with the affected tree id");
        Assert.That(
            sample.Value.Tags.Any(t => t.Key == LatticeMetrics.TagShard),
            Is.True,
            "batched-path deadline trip must be tagged with the affected shard index");

        release.TrySetResult(Array.Empty<long>());
    }

    [Test]
    public void AppendAsync_dispatch_timeout_increments_per_tree_shard_counter_consumed_by_saturation_sampler()
    {
        // The writer publishes a cumulative per-(tree, shard) dispatch-
        // timeout trip count into a static dictionary that the silo-
        // scoped WalSaturationSampler reads on each tick to derive the
        // dispatch-timeout half of the Saturated classification. This
        // test pins the writer-side increment contract: one trip
        // increments the (tree, shard) slot by exactly one, and the
        // shard component matches the writer partition the dispatch
        // targeted.
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();

        var release = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(release.Task);

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
        });

        Assert.That(
            async () => await writer.AppendAsync(MakeMutation()),
            Throws.TypeOf<TimeoutException>());

        // Locate the increment. The writer partition of the synthetic
        // mutation is deterministic for the configured tree id but
        // varies with the partition hash; rather than recomputing the
        // hash, the test asserts that exactly one slot in the dictionary
        // was incremented to 1 and that slot belongs to the test tree.
        var hits = WalCommitLogWriter._dispatchTimeoutCounts
            .Where(kv => kv.Key.TreeId == TreeId)
            .ToList();
        Assert.That(hits, Has.Count.EqualTo(1),
            "exactly one (tree, shard) slot must be incremented by a single dispatch-timeout trip");
        Assert.That(hits[0].Value, Is.EqualTo(1L),
            "first trip on a fresh slot must initialise the cumulative count to 1");

        release.TrySetResult(0L);
    }

    [Test]
    public void AppendAsync_repeated_dispatch_timeouts_accumulate_per_tree_shard_count()
    {
        // The counter is cumulative across the lifetime of the writer
        // singleton; the sampler subtracts the prior tick's reading to
        // derive a per-window delta. This test pins the "monotonic
        // counter" half of that contract: two trips on the same
        // (tree, shard) slot leave the count at 2.
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();

        var release = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(release.Task);

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(50),
        });

        Assert.That(async () => await writer.AppendAsync(MakeMutation("k1")), Throws.TypeOf<TimeoutException>());
        Assert.That(async () => await writer.AppendAsync(MakeMutation("k1")), Throws.TypeOf<TimeoutException>());

        // Both dispatches with the same key land on the same partition
        // (deterministic hash on the same string). The slot count must
        // therefore be 2 in exactly one slot for this tree.
        var hits = WalCommitLogWriter._dispatchTimeoutCounts
            .Where(kv => kv.Key.TreeId == TreeId)
            .ToList();
        Assert.That(hits, Has.Count.EqualTo(1),
            "two trips on the same key must land in exactly one (tree, shard) slot");
        Assert.That(hits[0].Value, Is.EqualTo(2L),
            "cumulative count must accumulate across trips on the same slot");

        release.TrySetResult(0L);
    }

    [Test]
    public async Task AppendAsync_successful_dispatch_does_not_touch_per_tree_shard_counter()
    {
        // The counter is a failure-tail signal; a happy-path dispatch
        // must not increment it, otherwise the sampler would observe a
        // false positive delta and classify a healthy tree as
        // Saturated.
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();

        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(42L));

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalAppendDispatchTimeout = TimeSpan.FromSeconds(5),
        });

        var offset = await writer.AppendAsync(MakeMutation());
        Assert.That(offset, Is.EqualTo(42L));

        Assert.That(
            WalCommitLogWriter._dispatchTimeoutCounts.Count(kv => kv.Key.TreeId == TreeId),
            Is.EqualTo(0),
            "happy-path dispatch must never touch the dispatch-timeout counter");
    }
}
