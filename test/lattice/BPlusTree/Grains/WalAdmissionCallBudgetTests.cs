using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// (#3348) Covers <see cref="LatticeOptions.WalAdmissionSaturationCallBudget"/>:
/// the bound on how long one <i>top-level call</i> may spend waiting at the WAL
/// admission saturation gate, summed across every append it makes.
/// <para>
/// The defect this closes is a multiplication, not a single long wait.
/// <see cref="LatticeOptions.WalAdmissionSaturationWaitBudget"/> bounds one
/// wait, but the write path holds three nested retry layers
/// (<c>RetryOnStaleRoutingAsync</c>, <c>ShardActivationRetry.RunAsync</c>,
/// <c>DispatchLeafBatchWithRetryAsync</c>) and each re-dispatch opened a fresh
/// one. The Layer 3 cohort logs show the consequence directly - "flush of 4096
/// failed after 5 retry attempts against LatticeSaturatedException; 10488ms of
/// that was saturation back-off" - and because <c>SetManyAsync</c> awaits every
/// shard, one such branch is paid by the whole batch.
/// </para>
/// <para>
/// These tests therefore assert on the <i>sum</i> across several appends in one
/// ambient call, which is the quantity that regressed. A per-append assertion
/// would have passed throughout the defect's lifetime.
/// </para>
/// </summary>
[TestFixture]
public class WalAdmissionCallBudgetTests
{
    private const int Partitions = 16;
    private const int HotPartition = 3;
    private static int _treeIdSeed;
    private string _treeId = null!;

    [SetUp]
    public void SetUp()
    {
        WalCommitLogWriter._trackers.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
        WalCommitLogWriter._providerFailureCounts.Clear();
        WalCommitLogWriter._walHeadWallClockTicks.Clear();
        // The call-start stamp is ambient, so a leaked value from a prior test
        // would silently hand this one a budget that is already spent.
        RequestContext.Clear();
        _treeId = $"tree-callbudget-{Interlocked.Increment(ref _treeIdSeed)}";
    }

    [TearDown]
    public void TearDown() => RequestContext.Clear();

    private static void SeedPartition(string treeId, int partition, int depth, int cap)
    {
        var tracker = new WalCommitLogWriter.PartitionTracker(treeId, partition);
        _ = tracker.AcquireAsync(cap, TimeSpan.FromMilliseconds(1), CancellationToken.None, CancellationToken.None)
            .GetAwaiter().GetResult();
        tracker.ReleaseAdmission();
        for (var i = 0; i < depth; i++)
        {
            var pending = new WalCommitLogWriter.PendingAppend(treeId, partition, entryCount: 1, batchBytes: 0);
            tracker.LinkReturningPreDepth(pending);
        }
        WalCommitLogWriter._trackers[(treeId, partition)] = tracker;
    }

    private static string KeyForPartition(int partition, int partitions)
    {
        for (var i = 0; i < 100_000; i++)
        {
            var candidate = $"k{i}";
            if (WalPartitionHash.Compute(candidate, partitions) == partition)
            {
                return candidate;
            }
        }
        throw new InvalidOperationException($"No key found routing to partition {partition} of {partitions}.");
    }

    private static WalSaturationSampler CreateSampler(
        LatticeOptions options,
        out WalSaturationSignal signal)
    {
        signal = new WalSaturationSignal();
        signal.ResetForTesting();
        options.WalSaturationRecoveryWindow = TimeSpan.Zero;

        // A partition seeded at its admission cap is the Saturated trigger
        // here (the historical classification). WalSaturationAcuteOnly
        // (default on, #3348) reads it as Throttled, so pin it off.
        options.WalSaturationAcuteOnly = false;

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);

        return new WalSaturationSampler(
            signal,
            new WalSaturationObserverDispatcher(
                Array.Empty<IWalSaturationObserver>(),
                NullLogger<WalSaturationObserverDispatcher>.Instance),
            monitor,
            NullLogger<WalSaturationSampler>.Instance,
            new InMemoryWalCursorRegistry());
    }

    private static WalCommitLogWriter CreateWriter(
        IWalSaturationSignal signal,
        LatticeOptions options)
    {
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(0L));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns("site-test");

        var optionsResolver = TestOptionsResolver.Create(baseOptions: options, factory: grainFactory);
        return new WalCommitLogWriter(
            grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver, signal);
    }

    private WalRecord MakeMutation(string key) => new()
    {
        TreeId = _treeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-test",
    };

    /// <summary>
    /// Saturates <see cref="HotPartition"/> and returns a writer plus a key
    /// routed at it, so every append in a test refuses at the gate.
    /// </summary>
    private async Task<(WalCommitLogWriter Writer, string Key)> ArrangeSaturatedAsync(LatticeOptions options)
    {
        for (var p = 0; p < Partitions; p++)
        {
            SeedPartition(_treeId, p, depth: p == HotPartition ? 16 : 0, cap: 16);
        }

        var sampler = CreateSampler(options, out var signal);
        await sampler.SampleOnceAsync(CancellationToken.None);

        return (CreateWriter(signal, options), KeyForPartition(HotPartition, Partitions));
    }

    private static LatticeOptions OptionsWith(TimeSpan appendBudget, TimeSpan callBudget) => new()
    {
        WalPartitions = Partitions,
        WalSaturationThrottledRatio = 0.75,
        WalSaturationDispatchTimeoutThreshold = 1,
        WalAdmissionSaturationWaitBudget = appendBudget,
        WalAdmissionSaturationCallBudget = callBudget,
    };

    /// <summary>
    /// The fix. Three appends inside one ambient call must share a single
    /// <see cref="LatticeOptions.WalAdmissionSaturationCallBudget"/> rather than
    /// each opening a fresh per-append wait, so the total is bounded by the call
    /// budget and not by (appends x per-append budget).
    /// </summary>
    [Test]
    public async Task Appends_in_one_call_share_the_call_budget_rather_than_each_taking_a_fresh_one()
    {
        var appendBudget = TimeSpan.FromMilliseconds(250);
        var callBudget = TimeSpan.FromMilliseconds(400);
        var (writer, key) = await ArrangeSaturatedAsync(OptionsWith(appendBudget, callBudget));

        // One top-level logical call covering all three appends.
        LatticeTransactionContext.EnsureCallStart();

        var sw = Stopwatch.StartNew();
        for (var i = 0; i < 3; i++)
        {
            Assert.ThrowsAsync<LatticeSaturatedException>(
                async () => await writer.AppendAsync(MakeMutation(key)));
        }
        sw.Stop();

        // Unbounded, this is 3 x 250ms = 750ms. Bounded, it is one 400ms
        // budget shared across all three. The margin is deliberately wide so
        // the assertion fails on the mechanism, not on scheduler jitter.
        Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(650)),
            "three appends in one call must not each buy a fresh per-append gate wait");
    }

    /// <summary>
    /// The control for the test above, and the guarantee that the default is
    /// non-breaking: with the call budget left at its
    /// <see cref="Timeout.InfiniteTimeSpan"/> default, each append waits its own
    /// per-append budget exactly as it did before #3348.
    /// </summary>
    [Test]
    public async Task Infinite_call_budget_leaves_each_append_its_own_wait()
    {
        var appendBudget = TimeSpan.FromMilliseconds(250);
        var (writer, key) = await ArrangeSaturatedAsync(
            OptionsWith(appendBudget, Timeout.InfiniteTimeSpan));

        Assert.That(LatticeOptions.DefaultWalAdmissionSaturationCallBudget,
            Is.EqualTo(Timeout.InfiniteTimeSpan),
            "the default must disable the per-call bound so the change is non-breaking");

        LatticeTransactionContext.EnsureCallStart();

        var sw = Stopwatch.StartNew();
        for (var i = 0; i < 3; i++)
        {
            Assert.ThrowsAsync<LatticeSaturatedException>(
                async () => await writer.AppendAsync(MakeMutation(key)));
        }
        sw.Stop();

        Assert.That(sw.Elapsed, Is.GreaterThan(TimeSpan.FromMilliseconds(700)),
            "with the per-call bound disabled each append must still wait its full per-append budget");
    }

    /// <summary>
    /// Once the call's share is spent the gate must refuse without opening
    /// another wait at all, and must say which bound it hit - the whole point is
    /// that an operator can tell a per-call refusal from a per-append one.
    /// </summary>
    [Test]
    public async Task Exhausted_call_budget_refuses_immediately_and_names_the_call_budget()
    {
        var (writer, key) = await ArrangeSaturatedAsync(
            OptionsWith(TimeSpan.FromMilliseconds(250), TimeSpan.FromMilliseconds(120)));

        LatticeTransactionContext.EnsureCallStart();

        // The call budget (120ms) is below the per-append budget (250ms), so
        // it is the binding constraint on this first wait even though it is
        // not yet spent. That must already be attributed to the per-call
        // bound: an operator told to raise WalAdmissionSaturationWaitBudget
        // here would be raising a knob the gate never reached.
        var truncated = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await writer.AppendAsync(MakeMutation(key)));
        Assert.That(truncated!.Message, Does.Contain(nameof(LatticeOptions.WalAdmissionSaturationCallBudget)),
            "a wait truncated by the call budget is a per-call refusal, not a per-append one");

        var sw = Stopwatch.StartNew();
        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await writer.AppendAsync(MakeMutation(key)));
        sw.Stop();

        Assert.Multiple(() =>
        {
            Assert.That(sw.Elapsed, Is.LessThan(TimeSpan.FromMilliseconds(150)),
                "an exhausted call budget must refuse without waiting again");
            Assert.That(ex!.Message, Does.Contain(nameof(LatticeOptions.WalAdmissionSaturationCallBudget)),
                "the refusal must attribute itself to the per-call bound, not the per-append one");
            Assert.That(ex.SaturationSource, Is.EqualTo(LatticeSaturationSource.WalAdmission),
                "the refusal is still the WAL admission seam");
        });
    }

    /// <summary>
    /// Convergence-only and background writes never pass through a public entry
    /// point, so they carry no ambient call-start stamp. They must keep the
    /// per-append bound rather than being refused instantly by a call budget
    /// measured from an epoch they never set.
    /// </summary>
    [Test]
    public async Task Without_an_ambient_call_start_the_per_append_budget_still_governs()
    {
        var appendBudget = TimeSpan.FromMilliseconds(200);
        var (writer, key) = await ArrangeSaturatedAsync(
            OptionsWith(appendBudget, TimeSpan.FromMilliseconds(400)));

        // Deliberately no EnsureCallStart(): this is the background path.
        Assert.That(LatticeTransactionContext.CallStartUtcTicks, Is.Null,
            "the arrangement must genuinely have no ambient call-start stamp");

        var sw = Stopwatch.StartNew();
        Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await writer.AppendAsync(MakeMutation(key)));
        sw.Stop();

        Assert.That(sw.Elapsed, Is.GreaterThan(TimeSpan.FromMilliseconds(120)),
            "a background append must still get its per-append wait, not an instant refusal");
    }

    /// <summary>
    /// The stamp identifies the <i>outermost</i> call. A nested entry-point that
    /// re-stamped it would hand the gate a fresh budget at every nesting level,
    /// which is exactly the multiplication being removed.
    /// </summary>
    [Test]
    public void EnsureCallStart_preserves_the_outermost_stamp()
    {
        var first = LatticeTransactionContext.EnsureCallStart();
        Thread.Sleep(5);
        var nested = LatticeTransactionContext.EnsureCallStart();

        Assert.Multiple(() =>
        {
            Assert.That(nested, Is.EqualTo(first),
                "a nested entry-point must inherit the outermost call's start instant");
            Assert.That(LatticeTransactionContext.CallStartUtcTicks, Is.EqualTo(first));
        });
    }

    /// <summary>
    /// <see cref="LatticeTransactionContext.EnsureCurrent"/> is what the eleven
    /// public write entry-points actually call, so the stamp has to be planted
    /// by that path and not only by the helper above.
    /// </summary>
    [Test]
    public void EnsureCurrent_plants_the_call_start_stamp()
    {
        Assert.That(LatticeTransactionContext.CallStartUtcTicks, Is.Null);

        LatticeTransactionContext.EnsureCurrent();

        Assert.That(LatticeTransactionContext.CallStartUtcTicks, Is.Not.Null,
            "every public write entry-point must establish the per-call budget epoch");
    }
}
