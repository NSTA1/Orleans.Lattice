using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// (#3348) <see cref="LatticeOptions.WalSaturationAcuteOnly"/>: an admission
/// semaphore merely at its cap is the steady state of a well-pipelined
/// partition, not an acute fault. With the option set it classifies
/// <c>Throttled</c> rather than <c>Saturated</c>, and a caller parked at the
/// writer's admission gate resumes once its partition leaves <c>Saturated</c>
/// rather than waiting for <c>Healthy</c>. Acute causes still saturate.
/// </summary>
[TestFixture]
public class WalSaturationAcuteOnlyTests
{
    private const int Partitions = 16;
    private static int _treeIdSeed;
    private string _treeId = null!;

    [SetUp]
    public void SetUp()
    {
        WalCommitLogWriter._trackers.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
        WalCommitLogWriter._providerFailureCounts.Clear();
        WalCommitLogWriter._walHeadWallClockTicks.Clear();
        _treeId = $"tree-acute-{Interlocked.Increment(ref _treeIdSeed)}";
    }

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

    private static WalSaturationSampler CreateSampler(LatticeOptions options, out WalSaturationSignal signal)
    {
        signal = new WalSaturationSignal();
        signal.ResetForTesting();
        options.WalSaturationRecoveryWindow = TimeSpan.Zero;

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

    private static WalCommitLogWriter CreateWriter(IWalSaturationSignal signal, LatticeOptions options)
    {
        var shard = Substitute.For<IWalShardGrain>();
        shard.StubPointAppend(Task.FromResult(0L));

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

    [Test]
    public void WalSaturationAcuteOnly_defaults_to_true()
    {
        Assert.That(new LatticeOptions().WalSaturationAcuteOnly, Is.True);
        Assert.That(LatticeOptions.DefaultWalSaturationAcuteOnly, Is.True);
    }

    [TestCase(false, WalSaturationState.Saturated)]
    [TestCase(true, WalSaturationState.Throttled)]
    public async Task Sampler_classifies_a_partition_at_cap_by_the_option(bool acuteOnly, WalSaturationState expected)
    {
        SeedPartition(_treeId, partition: 3, depth: 16, cap: 16);
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalPartitions = Partitions,
                WalSaturationThrottledRatio = 0.75,
                WalSaturationAcuteOnly = acuteOnly,
            },
            out var signal);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(signal.GetCurrentState(_treeId, 3), Is.EqualTo(expected), "partition verdict");
            Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(expected), "tree-wide verdict");
        });
    }

    [Test]
    public async Task Sampler_still_saturates_on_dispatch_timeouts_when_acute_only()
    {
        SeedPartition(_treeId, partition: 3, depth: 16, cap: 16);
        var sampler = CreateSampler(
            new LatticeOptions
            {
                WalPartitions = Partitions,
                WalSaturationThrottledRatio = 0.75,
                WalSaturationDispatchTimeoutThreshold = 1,
                WalSaturationAcuteOnly = true,
            },
            out var signal);

        // First tick is baseline-only for the dispatch-timeout delta.
        await sampler.SampleOnceAsync(CancellationToken.None);
        WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 3)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId, 3), Is.EqualTo(WalSaturationState.Saturated),
            "an acute cause must still saturate the partition; only the at-cap heuristic is relaxed");
    }

    [Test]
    public void WaitForNotSaturatedAsync_throws_on_null_treeId()
    {
        var signal = new WalSaturationSignal();
        Assert.Throws<ArgumentNullException>(() => signal.WaitForNotSaturatedAsync(null!, 0));
    }

    [Test]
    public void WaitForNotSaturatedAsync_completes_synchronously_when_Throttled()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdatePartitionState(_treeId, 0, WalSaturationState.Throttled);

        Assert.That(signal.WaitForNotSaturatedAsync(_treeId, 0).IsCompleted, Is.True);
    }

    [Test]
    public async Task Throttled_tick_releases_not_saturated_waiters_but_not_healthy_waiters()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdatePartitionState(_treeId, 0, WalSaturationState.Saturated);

        var notSaturated = signal.WaitForNotSaturatedAsync(_treeId, 0);
        var healthy = signal.WaitForHealthyAsync(_treeId, 0);
        Assert.That(notSaturated.IsCompleted || healthy.IsCompleted, Is.False, "both must park while Saturated");

        signal.UpdatePartitionState(_treeId, 0, WalSaturationState.Throttled);

        await notSaturated.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(healthy.IsCompleted, Is.False,
            "a Throttled tick must not release a caller that asked for Healthy");

        signal.UpdatePartitionState(_treeId, 0, WalSaturationState.Healthy);
        await healthy.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Test]
    public void Not_saturated_release_is_paced_by_the_release_batch()
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdatePartitionState(_treeId, 0, WalSaturationState.Saturated);

        var waits = Enumerable.Range(0, 5)
            .Select(_ => signal.WaitForNotSaturatedAsync(_treeId, 0))
            .ToArray();

        signal.UpdatePartitionState(_treeId, 0, WalSaturationState.Throttled, releaseBatch: 2);
        Assert.That(waits.Count(w => w.IsCompleted), Is.EqualTo(2), "first tick releases one batch");

        signal.UpdatePartitionState(_treeId, 0, WalSaturationState.Throttled, releaseBatch: 2);
        Assert.That(waits.Count(w => w.IsCompleted), Is.EqualTo(4),
            "a steady Throttled tick keeps draining the residue (level-triggered)");
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Gated_append_resumes_on_Throttled_only_when_acute_only(bool acuteOnly)
    {
        const int partition = 5;
        var options = new LatticeOptions
        {
            WalPartitions = Partitions,
            WalAdmissionSaturationWaitBudget = TimeSpan.FromSeconds(30),
            WalSaturationAcuteOnly = acuteOnly,
        };
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        signal.UpdateState(_treeId, WalSaturationState.Saturated);
        signal.UpdatePartitionState(_treeId, partition, WalSaturationState.Saturated);

        var writer = CreateWriter(signal, options);
        var append = writer.AppendAsync(MakeMutation(KeyForPartition(partition, Partitions)));
        await Task.Delay(50);
        Assert.That(append.IsCompleted, Is.False, "precondition: the append parks at the gate while Saturated");

        signal.UpdateState(_treeId, WalSaturationState.Throttled);
        signal.UpdatePartitionState(_treeId, partition, WalSaturationState.Throttled);

        if (acuteOnly)
        {
            var offset = await append.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.That(offset, Is.EqualTo(0L), "acute-only: leaving Saturated is enough to resume");
            return;
        }

        await Task.Delay(200);
        Assert.That(append.IsCompleted, Is.False,
            "default: the parked append keeps waiting for Healthy through a Throttled tick");

        signal.UpdateState(_treeId, WalSaturationState.Healthy);
        signal.UpdatePartitionState(_treeId, partition, WalSaturationState.Healthy);
        Assert.That(await append.WaitAsync(TimeSpan.FromSeconds(5)), Is.EqualTo(0L));
    }
}
