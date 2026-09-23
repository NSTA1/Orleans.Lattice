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
/// (#3348) Cross-partition coupling in the WAL admission gate.
/// <para>
/// The sampler collapses every live partition's admission depth into a
/// single per-tree worst case (<c>max(depth_ratio)</c>, and <c>any</c>
/// for parked callers), and the writer's pre-admission gate consults
/// only that per-tree verdict - <c>GateOnSaturationAsync</c> receives a
/// partition but uses it purely for metric and exception attribution.
/// One partition at cap therefore refuses appends to every other
/// partition of the same tree, including partitions that are provably
/// idle.
/// </para>
/// <para>
/// That coupling is the scatter-gather tail amplification of #3348 one
/// layer below the <c>LatticeGrain</c> fan-out: with per-partition
/// busy probability <c>p</c> over <c>B</c> partitions, the tree reads
/// Saturated with probability <c>1-(1-p)^B</c>, which crosses its knee
/// as offered load rises and holds the whole tree refusing while the
/// WAL itself is measurably starved.
/// </para>
/// </summary>
[TestFixture]
public class WalSaturationCrossPartitionBlockingTests
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
        _treeId = $"tree-xpart-{Interlocked.Increment(ref _treeIdSeed)}";
    }

    /// <summary>
    /// Installs a partition tracker primed to the given in-flight depth
    /// and admission cap, mirroring <c>WalSaturationSamplerTests</c>.
    /// </summary>
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

    /// <summary>
    /// Finds a key that the WAL partition hash routes to
    /// <paramref name="partition"/>, so a test can drive an append at a
    /// chosen partition without reaching through the routing layer.
    /// </summary>
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

        // Zero the recovery window so classification is a deterministic
        // function of this tick's depths alone. The window only latches
        // the verdict for longer; it is not what creates the coupling.
        options.WalSaturationRecoveryWindow = TimeSpan.Zero;

        // A partition seeded at its admission cap is the Saturated trigger
        // here, which is the historical classification. WalSaturationAcuteOnly
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
    /// One partition at cap must not make the other fifteen unusable.
    /// This is the defect: an append routed to a provably idle
    /// partition is refused because the gate reads the tree-wide max.
    /// </summary>
    [Test]
    public async Task AppendAsync_to_idle_partition_is_admitted_while_a_different_partition_is_saturated()
    {
        const int hotPartition = 3;

        // One partition pinned at cap; every other partition idle.
        for (var p = 0; p < Partitions; p++)
        {
            SeedPartition(_treeId, p, depth: p == hotPartition ? 16 : 0, cap: 16);
        }

        var options = new LatticeOptions
        {
            WalPartitions = Partitions,
            WalSaturationThrottledRatio = 0.75,
            WalSaturationDispatchTimeoutThreshold = 1,
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(200),
        };

        var sampler = CreateSampler(options, out var signal);
        await sampler.SampleOnceAsync(CancellationToken.None);

        // Precondition: the tree-wide verdict is Saturated, driven
        // entirely by the single hot partition.
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "precondition: one partition at cap must drive the tree-wide verdict to Saturated");

        var writer = CreateWriter(signal, options);

        // Route to an idle partition. This partition has a full
        // admission cap available and nothing queued against it.
        var idlePartition = (hotPartition + 1) % Partitions;
        var key = KeyForPartition(idlePartition, Partitions);
        Assert.That(WalPartitionHash.Compute(key, Partitions), Is.EqualTo(idlePartition),
            "test key must route to the chosen idle partition");

        var offset = await writer.AppendAsync(MakeMutation(key));

        Assert.That(offset, Is.EqualTo(0L),
            "an append to an idle partition must be admitted; the saturation of partition "
            + $"{hotPartition} is not a property of partition {idlePartition}");
    }

    /// <summary>
    /// The append routed at the genuinely saturated partition must
    /// still be refused - the fix narrows the gate, it does not remove
    /// it.
    /// </summary>
    [Test]
    public async Task AppendAsync_to_the_saturated_partition_is_still_refused()
    {
        const int hotPartition = 3;

        for (var p = 0; p < Partitions; p++)
        {
            SeedPartition(_treeId, p, depth: p == hotPartition ? 16 : 0, cap: 16);
        }

        var options = new LatticeOptions
        {
            WalPartitions = Partitions,
            WalSaturationThrottledRatio = 0.75,
            WalSaturationDispatchTimeoutThreshold = 1,
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(200),
        };

        var sampler = CreateSampler(options, out var signal);
        await sampler.SampleOnceAsync(CancellationToken.None);

        var writer = CreateWriter(signal, options);
        var key = KeyForPartition(hotPartition, Partitions);

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await writer.AppendAsync(MakeMutation(key)));

        Assert.That(ex!.SaturationSource, Is.EqualTo(LatticeSaturationSource.WalAdmission),
            "the refusal must still be attributed to the WAL admission seam");
    }

    /// <summary>
    /// The guarantee that narrowing the gate loses nothing: a cause
    /// that is genuinely tree-wide rather than partition-local - here a
    /// burst of dispatch-timeout trips - must still gate <i>every</i>
    /// partition, including ones whose admission depth is zero.
    /// <para>
    /// This is the other half of the fix. Partition-scoping the gate
    /// would be a regression if it also discarded the tree-wide
    /// classification inputs (dispatch timeouts, provider failures,
    /// flush latency, drain lag); the sampler therefore re-runs the
    /// same classifier per partition with only the admission-depth and
    /// parked-caller terms substituted, so a tree-wide cause floors
    /// every partition at the tree's own verdict.
    /// </para>
    /// </summary>
    [Test]
    public async Task Tree_wide_dispatch_timeouts_still_gate_every_partition()
    {
        // Every partition idle: nothing partition-local is wrong.
        for (var p = 0; p < Partitions; p++)
        {
            SeedPartition(_treeId, p, depth: 0, cap: 16);
        }

        var options = new LatticeOptions
        {
            WalPartitions = Partitions,
            WalSaturationThrottledRatio = 0.75,
            WalSaturationDispatchTimeoutThreshold = 1,
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(200),
        };

        var sampler = CreateSampler(options, out var signal);

        // First tick establishes the dispatch-timeout baseline without
        // firing transitions; the sampler deliberately does not
        // double-count history accumulated before it started.
        await sampler.SampleOnceAsync(CancellationToken.None);

        // A tree-wide cause: a burst of dispatch-timeout trips against
        // one shard, well past the threshold.
        WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 0)] = 5;
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "precondition: a dispatch-timeout burst must drive the tree-wide verdict to Saturated");

        // Every partition must inherit that verdict, despite each one
        // being individually idle.
        for (var p = 0; p < Partitions; p++)
        {
            Assert.That(signal.GetCurrentState(_treeId, p), Is.EqualTo(WalSaturationState.Saturated),
                $"partition {p} must inherit the tree-wide cause: dispatch-timeout trips are not "
                + "a property of any single partition");
        }

        // And the gate must actually refuse on an idle partition.
        var writer = CreateWriter(signal, options);
        var idleKey = KeyForPartition(7, Partitions);

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await writer.AppendAsync(MakeMutation(idleKey)));

        Assert.That(ex!.SaturationSource, Is.EqualTo(LatticeSaturationSource.WalAdmission));
    }

    /// <summary>
    /// The fail-closed fallback. The partition-scoped surface is an
    /// internal interface rather than a default interface method on the
    /// public <see cref="IWalSaturationSignal"/> precisely so that an
    /// implementation which does not carry it - a foreign one, or one
    /// standing behind a dynamic proxy such as this substitute - falls
    /// back to the tree-wide gate rather than silently reporting every
    /// partition healthy.
    /// <para>
    /// A default interface method would be overridden by the proxy's
    /// generated stub, which returns <c>default</c>; because
    /// <see cref="WalSaturationState.Healthy"/> is <c>0</c>, that would
    /// remove WAL back-pressure altogether - a fail-<i>open</i>
    /// admission gate. This test pins the opposite behaviour.
    /// </para>
    /// </summary>
    [Test]
    public void Signal_without_partition_surface_falls_back_to_tree_wide_gating()
    {
        var options = new LatticeOptions
        {
            WalPartitions = Partitions,
            WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(50),
        };

        // A substitute implements only the public interface, so the
        // writer's type test for the internal partition surface fails
        // and the gate must fall back to the tree-wide verdict.
        var signal = Substitute.For<IWalSaturationSignal>();
        signal.GetCurrentState(Arg.Any<string>()).Returns(WalSaturationState.Saturated);
        signal.WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.Delay(Timeout.Infinite, ci.Arg<CancellationToken>()));

        Assert.That(signal, Is.Not.InstanceOf<IWalPartitionSaturationSignal>(),
            "precondition: the substitute must not carry the internal partition surface");

        var writer = CreateWriter(signal, options);
        var key = KeyForPartition(7, Partitions);

        var ex = Assert.ThrowsAsync<LatticeSaturatedException>(
            async () => await writer.AppendAsync(MakeMutation(key)));

        Assert.That(ex!.SaturationSource, Is.EqualTo(LatticeSaturationSource.WalAdmission),
            "an implementation without the partition surface must keep the pre-#3348 tree-wide "
            + "gating, not lose back-pressure");

        // And the fallback must have consulted the tree-wide overloads.
        signal.Received().GetCurrentState(Arg.Any<string>());
        _ = signal.Received().WaitForHealthyAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Seeds one partition into Throttled (depth ratio at the throttle
    /// threshold but below cap, so the admission gate does not refuse)
    /// and leaves every other partition idle.
    /// </summary>
    private WalSaturationSampler SeedOneThrottledPartition(
        int hotPartition,
        LatticeOptions options,
        out WalSaturationSignal signal)
    {
        for (var p = 0; p < Partitions; p++)
        {
            SeedPartition(_treeId, p, depth: p == hotPartition ? 12 : 0, cap: 16);
        }

        return CreateSampler(options, out signal);
    }

    private static LatticeOptions PacingOptions(TimeSpan pace) => new()
    {
        WalPartitions = Partitions,
        WalSaturationThrottledRatio = 0.75,
        WalSaturationDispatchTimeoutThreshold = 1,
        WalAdmissionSaturationWaitBudget = TimeSpan.FromMilliseconds(200),
        WalThrottledAdmissionPace = pace,
    };

    /// <summary>
    /// (#3348) Throttled <i>pacing</i> must be partition-scoped for the
    /// same reason the refusal gate is.
    /// <para>
    /// <c>PaceOnThrottleAsync</c> runs on every append, one line below
    /// <c>GateOnSaturationAsync</c>, and charged a bounded delay to
    /// every partition whenever the tree-wide roll-up read Throttled.
    /// Because that roll-up is a <c>max</c> across partitions, a single
    /// busy partition taxed all <c>B</c> of them - the same coupling as
    /// the refusal gate, differing only in that it burns latency rather
    /// than failing the call, which is why it survived the first fix.
    /// </para>
    /// </summary>
    [Test]
    public async Task AppendAsync_to_idle_partition_is_not_paced_while_a_different_partition_is_throttled()
    {
        const int hotPartition = 3;

        // A pace far larger than the assertion ceiling, so a tree-wide
        // read cannot pass this test by being merely fast.
        var options = PacingOptions(TimeSpan.FromSeconds(5));
        var sampler = SeedOneThrottledPartition(hotPartition, options, out var signal);
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "precondition: one partition at the throttle ratio must drive the tree-wide verdict "
            + "to Throttled (and not to Saturated, which the refusal gate would absorb first)");

        var writer = CreateWriter(signal, options);
        var idlePartition = (hotPartition + 1) % Partitions;
        var key = KeyForPartition(idlePartition, Partitions);

        var started = System.Diagnostics.Stopwatch.StartNew();
        _ = await writer.AppendAsync(MakeMutation(key));
        started.Stop();

        Assert.That(started.Elapsed, Is.LessThan(TimeSpan.FromSeconds(1)),
            $"an append to idle partition {idlePartition} must not be paced by the throttling of "
            + $"partition {hotPartition}; a 5s pace was charged, so anything near it means the "
            + "pace still reads the tree-wide roll-up");
    }

    /// <summary>
    /// The converse: narrowing the pace must not disable it. An append
    /// routed at the genuinely Throttled partition is still paced.
    /// </summary>
    [Test]
    public async Task AppendAsync_to_the_throttled_partition_is_still_paced()
    {
        const int hotPartition = 3;

        var options = PacingOptions(TimeSpan.FromMilliseconds(400));
        var sampler = SeedOneThrottledPartition(hotPartition, options, out var signal);
        await sampler.SampleOnceAsync(CancellationToken.None);

        var writer = CreateWriter(signal, options);
        var key = KeyForPartition(hotPartition, Partitions);

        var started = System.Diagnostics.Stopwatch.StartNew();
        _ = await writer.AppendAsync(MakeMutation(key));
        started.Stop();

        Assert.That(started.Elapsed, Is.GreaterThan(TimeSpan.FromMilliseconds(250)),
            $"partition {hotPartition} is genuinely Throttled, so its own appends must still pay "
            + "the pace - narrowing the scope must not silently delete the back-pressure");
    }
}
