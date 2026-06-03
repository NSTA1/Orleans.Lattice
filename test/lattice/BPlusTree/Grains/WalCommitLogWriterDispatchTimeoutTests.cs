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
}
