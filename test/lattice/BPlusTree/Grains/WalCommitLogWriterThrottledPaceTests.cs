using System.Diagnostics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the local-path Throttled pacing inside
/// <see cref="WalCommitLogWriter"/>. When the per-tree
/// <see cref="IWalSaturationSignal"/> reports
/// <see cref="WalSaturationState.Throttled"/> the writer applies a
/// single bounded <see cref="System.Threading.Tasks.Task.Delay(System.TimeSpan, System.Threading.CancellationToken)"/>
/// of <see cref="LatticeOptions.WalThrottledAdmissionPace"/> before
/// admitting into the per-partition admission semaphore, giving the
/// drain-lag back-pressure teeth on the single-silo local-write path.
/// It is a pure back-off: it never throws a saturation fault.
/// </summary>
[TestFixture]
public class WalCommitLogWriterThrottledPaceTests
{
    private const string TreeId = "tree-throttlepace";

    // A pace large enough that the delay is unambiguously observable
    // over scheduler jitter, but small enough to keep the suite fast.
    private static readonly TimeSpan Pace = TimeSpan.FromMilliseconds(300);

    // A lower-bound tolerance below the configured pace so the timing
    // assertion is robust against Task.Delay rounding / early wakeups.
    private static readonly TimeSpan PaceLowerBound = TimeSpan.FromMilliseconds(150);

    // An upper bound a Healthy / disabled-pace dispatch must beat so a
    // regression that paces unconditionally lights up here.
    private static readonly TimeSpan FastUpperBound = TimeSpan.FromMilliseconds(120);

    [SetUp]
    public void SetUp()
    {
        WalCommitLogWriter._trackers.Clear();
        WalCommitLogWriter._providerFailureCounts.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
    }

    private static WalCommitLogWriter CreateWriter(
        IWalShardGrain shard,
        IWalSaturationSignal? signal,
        LatticeOptions? options = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");

        var optionsResolver = TestOptionsResolver.Create(baseOptions: optionsMonitor.Get(string.Empty), factory: grainFactory);
        return new WalCommitLogWriter(grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver, signal);
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

    private static IWalShardGrain CreateHealthyShard()
    {
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(0L));
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var list = (IReadOnlyList<WalRecord>)callInfo[0];
                var offsets = new long[list.Count];
                for (var i = 0; i < offsets.Length; i++) offsets[i] = i;
                return Task.FromResult<IReadOnlyList<long>>(offsets);
            });
        return shard;
    }

    [Test]
    public async Task AppendAsync_signal_Throttled_paces_the_local_dispatch()
    {
        // The canonical local-path back-off: a Throttled tree must slow
        // the producer by the configured pace before admission, so the
        // materialiser drain can catch up on the single-silo write path
        // where no remote sender exists to drip-feed.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Throttled);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalThrottledAdmissionPace = Pace,
        });

        var sw = Stopwatch.StartNew();
        var offset = await writer.AppendAsync(MakeMutation());
        sw.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(offset, Is.EqualTo(0L), "pacing is a pure back-off; the append must still succeed");
            Assert.That(sw.Elapsed, Is.GreaterThanOrEqualTo(PaceLowerBound),
                "a Throttled tree must incur the configured pacing delay before admission");
        });
    }

    [Test]
    public async Task AppendAsync_signal_Healthy_does_not_pace()
    {
        // The Healthy fast path must be a single dictionary lookup and a
        // direct return - no Task.Delay - so steady-state throughput is
        // unaffected by the pacing surface.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Healthy);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalThrottledAdmissionPace = Pace,
        });

        var sw = Stopwatch.StartNew();
        await writer.AppendAsync(MakeMutation());
        sw.Stop();

        Assert.That(sw.Elapsed, Is.LessThan(FastUpperBound),
            "a Healthy tree must not incur the Throttled pacing delay");
    }

    [Test]
    public async Task AppendAsync_zero_pace_disables_local_back_off_even_when_Throttled()
    {
        // Pace=Zero is the operator opt-out: even a Throttled tree must
        // dispatch without the local delay.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Throttled);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalThrottledAdmissionPace = TimeSpan.Zero,
        });

        var sw = Stopwatch.StartNew();
        await writer.AppendAsync(MakeMutation());
        sw.Stop();

        Assert.That(sw.Elapsed, Is.LessThan(FastUpperBound),
            "Pace=Zero must disable local pacing entirely");
    }

    [Test]
    public async Task AppendAsync_no_signal_registered_does_not_pace()
    {
        // The signal is optional; unit-test / single-node writers build
        // without it. The pacing surface must be a silent no-op then.
        var shard = CreateHealthyShard();

        var writer = CreateWriter(shard, signal: null, options: new LatticeOptions
        {
            WalThrottledAdmissionPace = Pace,
        });

        var sw = Stopwatch.StartNew();
        var offset = await writer.AppendAsync(MakeMutation());
        sw.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(offset, Is.EqualTo(0L));
            Assert.That(sw.Elapsed, Is.LessThan(FastUpperBound),
                "no-signal writers must not pace");
        });
    }

    [Test]
    public async Task AppendAsync_signal_Throttled_never_throws()
    {
        // The defining contract: Throttled is a back-off, not a fault.
        // The local pacing must slow the caller and then let it through;
        // it must never surface LatticeSaturatedException.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Throttled);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalThrottledAdmissionPace = TimeSpan.FromMilliseconds(20),
        });

        // Must complete normally - no exception of any saturation shape.
        var offset = await writer.AppendAsync(MakeMutation());
        Assert.That(offset, Is.EqualTo(0L));
    }

    [Test]
    public async Task AppendManyAsync_signal_Throttled_paces_the_batched_dispatch()
    {
        // The batched path is wired identically to the single-entry
        // path; a separate test pins the symmetry so a future refactor
        // that drops pacing from one path lights up here.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Throttled);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalThrottledAdmissionPace = Pace,
        });

        var batch = new List<WalRecord> { MakeMutation("a"), MakeMutation("b") };

        var sw = Stopwatch.StartNew();
        var offsets = await writer.AppendManyAsync(batch);
        sw.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(offsets, Has.Count.EqualTo(2));
            Assert.That(sw.Elapsed, Is.GreaterThanOrEqualTo(PaceLowerBound),
                "a Throttled tree must pace the batched dispatch too");
        });
    }

    [Test]
    public void AppendAsync_caller_cancellation_during_pace_surfaces_OperationCanceledException()
    {
        // Caller-driven cancellation during the pace must surface as
        // OperationCanceledException (the caller asked to abandon), not
        // be swallowed by the back-off.
        var shard = CreateHealthyShard();
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Throttled);

        var writer = CreateWriter(shard, signal, options: new LatticeOptions
        {
            WalThrottledAdmissionPace = TimeSpan.FromSeconds(30),
        });

        using var cts = new CancellationTokenSource();
        var task = writer.AppendAsync(MakeMutation(), cts.Token);
        cts.CancelAfter(TimeSpan.FromMilliseconds(20));

        var ex = Assert.CatchAsync(async () => await task);
        Assert.That(ex, Is.InstanceOf<OperationCanceledException>(),
            "caller cancellation during the pace must surface OperationCanceledException");
    }
}
