using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalSaturationSampler"/> covering each leg
/// of the three-state classification (admission-depth Throttled,
/// admission-depth Saturated, dispatch-timeout Saturated), the
/// transition counter side-effect, observer fan-out wiring, and idle
/// no-op behaviour. The sampler is driven one tick at a time via
/// <see cref="WalSaturationSampler.SampleOnceAsync"/> so the tests
/// never depend on wall-clock timing.
/// </summary>
[TestFixture]
public class WalSaturationSamplerTests
{
    private static int _treeIdSeed;
    private string _treeId = null!;

    [SetUp]
    public void SetUp()
    {
        // Hermetic isolation: each test uses a unique tree id and a
        // freshly-cleared partition / dispatch-timeout / provider-
        // failure tracker map so cross-test concurrency (NUnit's
        // per-fixture parallelism on CI) cannot share static state.
        WalCommitLogWriter._trackers.Clear();
        WalCommitLogWriter._dispatchTimeoutCounts.Clear();
        WalCommitLogWriter._providerFailureCounts.Clear();
        _treeId = $"tree-sampler-{Interlocked.Increment(ref _treeIdSeed)}";
    }

    private sealed class MeterCapture : IDisposable
    {
        private readonly MeterListener _listener;
        public ConcurrentBag<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> Records { get; } = new();

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
                },
            };
            _listener.SetMeasurementEventCallback<long>(
                (inst, value, tags, _) => Records.Add((inst.Name, value, tags.ToArray())));
            _listener.Start();
        }

        public long Sum(string instrumentName) =>
            Records.Where(r => r.Name == instrumentName).Sum(r => r.Value);

        public IEnumerable<(string Name, long Value, KeyValuePair<string, object?>[] Tags)> For(string instrumentName) =>
            Records.Where(r => r.Name == instrumentName);

        public void Dispose() => _listener.Dispose();
    }

    private WalSaturationSampler CreateSampler(
        LatticeOptions? options,
        IEnumerable<IWalSaturationObserver>? observers,
        out WalSaturationSignal signal,
        out WalSaturationObserverDispatcher dispatcher)
    {
        signal = new WalSaturationSignal();
        signal.ResetForTesting();
        dispatcher = new WalSaturationObserverDispatcher(
            observers ?? Array.Empty<IWalSaturationObserver>(),
            NullLogger<WalSaturationObserverDispatcher>.Instance);

        // Always zero the saturation-classifier recovery window in
        // the shared test factory so tests written before the
        // recovery-window upgrade (which assert direct
        // Saturated -> Healthy transitions on the next tick after a
        // synthetic drain) continue to exercise the deterministic
        // per-tick classifier behaviour the sampler shipped with.
        // Recovery-window tests that exercise the upgrade explicitly
        // construct their sampler via a separate helper that does
        // not zero the window (see
        // CreateRecoveryWindowSampler below).
        var effective = options ?? new LatticeOptions();
        effective.WalSaturationRecoveryWindow = TimeSpan.Zero;

        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(effective);

        return new WalSaturationSampler(
            signal,
            dispatcher,
            monitor,
            NullLogger<WalSaturationSampler>.Instance);
    }

    /// <summary>
    /// Installs a partition tracker for the given (tree, partition)
    /// and primes it to the given in-flight depth and cap. The
    /// partition becomes visible to the sampler via the static
    /// <see cref="WalCommitLogWriter._trackers"/> map.
    /// </summary>
    private static void SeedPartition(string treeId, int partition, int depth, int cap)
    {
        var tracker = new WalCommitLogWriter.PartitionTracker(treeId, partition);
        // Prime the admission semaphore to expose the configured cap
        // to SnapshotDepth(). The acquire path lazy-initialises the
        // semaphore on first AcquireAsync; we drive that directly so
        // the tests do not pay the overhead of routing through the
        // full writer pipeline.
        // ReSharper disable once VSTHRD002 - sync drive is fine in tests.
        _ = tracker.AcquireAsync(cap, TimeSpan.FromMilliseconds(1), CancellationToken.None, CancellationToken.None).GetAwaiter().GetResult();
        // We hold one slot from the warm-up acquire; release it so the
        // tracker is at zero before we link the test depth.
        tracker.ReleaseAdmission();
        // Link `depth` pending stamps to bring the in-flight depth up.
        for (var i = 0; i < depth; i++)
        {
            var pending = new WalCommitLogWriter.PendingAppend(treeId, partition, entryCount: 1, batchBytes: 0);
            tracker.LinkReturningPreDepth(pending);
        }
        WalCommitLogWriter._trackers[(treeId, partition)] = tracker;
    }

    [Test]
    public async Task SampleOnceAsync_returns_Healthy_when_depth_well_below_throttled_ratio()
    {
        SeedPartition(_treeId, partition: 0, depth: 2, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationThrottledRatio = 0.75, WalSaturationDispatchTimeoutThreshold = 1 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "depth 2 / cap 16 = 0.125 is well below the 0.75 throttled ratio");
    }

    [Test]
    public async Task SampleOnceAsync_returns_Throttled_when_depth_at_or_above_ratio_but_below_cap()
    {
        SeedPartition(_treeId, partition: 0, depth: 12, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationThrottledRatio = 0.75 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "depth 12 / cap 16 = 0.75 must hit the throttled threshold exactly");
    }

    [Test]
    public async Task SampleOnceAsync_returns_Saturated_when_depth_reaches_cap()
    {
        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationThrottledRatio = 0.75 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "depth at-cap with HasParkedCallers=true must escalate straight to Saturated");
    }

    [Test]
    public async Task SampleOnceAsync_returns_Saturated_when_dispatch_timeout_delta_crosses_threshold()
    {
        // Healthy depth so the admission path stays at Healthy; the
        // dispatch-timeout path is the sole source of the transition.
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationDispatchTimeoutThreshold = 1 },
            observers: null,
            out var signal,
            out _);

        // First tick: initialises the dispatch-timeout baseline. No
        // transitions fire because the delta-from-prior is undefined
        // until the second tick.
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "first tick is baseline-only");

        // Simulate a dispatch-timeout trip on shard 5 between ticks.
        WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 5)] = 1;

        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "a single dispatch-timeout trip in the second tick must cross the default threshold of 1");
    }

    [Test]
    public async Task SampleOnceAsync_dispatch_timeout_delta_is_per_window_not_cumulative()
    {
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationDispatchTimeoutThreshold = 1 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // Tick 2: one trip, signal saturates.
        WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 5)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));

        // Tick 3: no new trips, signal recovers to Healthy (depth=0).
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "with no new trips and healthy depth, the signal must recover - the delta is per-window, not cumulative");
    }

    [Test]
    public async Task SampleOnceAsync_aggregates_worst_case_across_partitions_of_same_tree()
    {
        // Partition 0 healthy, partition 1 at cap. Tree state is the
        // worst case across its partitions.
        SeedPartition(_treeId, partition: 0, depth: 1, cap: 16);
        SeedPartition(_treeId, partition: 1, depth: 16, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationThrottledRatio = 0.75 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "tree state must be the worst case across its partitions");
    }

    [Test]
    public async Task SampleOnceAsync_keeps_trees_independent()
    {
        var otherTree = _treeId + "-other";
        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16); // Saturated
        SeedPartition(otherTree, partition: 0, depth: 2, cap: 16); // Healthy
        var sampler = CreateSampler(
            options: null,
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));
        Assert.That(signal.GetCurrentState(otherTree), Is.EqualTo(WalSaturationState.Healthy),
            "per-tree resolution: a saturated tree must not pollute a peer tree's signal");
    }

    [Test]
    public async Task SampleOnceAsync_fires_transition_counter_with_attribution_tags()
    {
        using var capture = new MeterCapture();
        SeedPartition(_treeId, partition: 3, depth: 12, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationThrottledRatio = 0.75 },
            observers: null,
            out _,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(capture.Sum("orleans.lattice.wal.saturation.transitions"), Is.EqualTo(1L),
            "first transition from implicit Healthy to Throttled must fire the counter exactly once");

        var sample = capture.For("orleans.lattice.wal.saturation.transitions").Single();
        Assert.Multiple(() =>
        {
            Assert.That(sample.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == _treeId), Is.True,
                "transition must be tagged with the tree id");
            Assert.That(sample.Tags.Any(t => t.Key == LatticeMetrics.TagWalSaturationState && (string?)t.Value == "throttled"), Is.True,
                "transition tag value for the new state must be the lowercased enum name");
            Assert.That(sample.Tags.Any(t => t.Key == LatticeMetrics.TagWalSaturationPreviousState && (string?)t.Value == "healthy"), Is.True,
                "transition must carry the previous state for direction filtering");
            Assert.That(sample.Tags.Any(t => t.Key == LatticeMetrics.TagPartition && t.Value is int p && p == 3), Is.True,
                "depth-driven transition must be attributed to the dominant partition");
        });
    }

    [Test]
    public async Task SampleOnceAsync_does_not_fire_counter_when_state_unchanged()
    {
        using var capture = new MeterCapture();
        SeedPartition(_treeId, partition: 0, depth: 2, cap: 16);
        var sampler = CreateSampler(
            options: null,
            observers: null,
            out _,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);
        await sampler.SampleOnceAsync(CancellationToken.None);
        await sampler.SampleOnceAsync(CancellationToken.None);

        // Tree starts at implicit Healthy; first tick observes Healthy.
        // UpdateState reports previous == new, so no transition fires.
        Assert.That(capture.Sum("orleans.lattice.wal.saturation.transitions"), Is.EqualTo(0L),
            "steady-state Healthy ticks must not fire any transitions");
    }

    [Test]
    public async Task SampleOnceAsync_dispatches_to_registered_observers_on_transition()
    {
        var recorder = new RecordingWalSaturationObserver();
        SeedPartition(_treeId, partition: 4, depth: 16, cap: 16);
        var sampler = CreateSampler(
            options: null,
            observers: new[] { (IWalSaturationObserver)recorder },
            out _,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(recorder.Changes, Has.Count.EqualTo(1));
        var change = recorder.Changes[0];
        Assert.Multiple(() =>
        {
            Assert.That(change.TreeId, Is.EqualTo(_treeId));
            Assert.That(change.PreviousState, Is.EqualTo(WalSaturationState.Healthy));
            Assert.That(change.NewState, Is.EqualTo(WalSaturationState.Saturated));
            Assert.That(change.AttributedPartition, Is.EqualTo(4));
            Assert.That(change.ObservedAt, Is.Not.EqualTo(default(DateTimeOffset)));
        });
    }

    [Test]
    public async Task SampleOnceAsync_is_zero_cost_when_no_partitions_seen()
    {
        using var capture = new MeterCapture();
        var sampler = CreateSampler(options: null, observers: null, out var signal, out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(capture.Sum("orleans.lattice.wal.saturation.transitions"), Is.EqualTo(0L),
            "no partitions, no transitions");
        Assert.That(signal.GetAggregateState(), Is.EqualTo(WalSaturationState.Healthy));
    }

    [Test]
    public async Task SampleOnceAsync_recovery_to_Healthy_completes_pending_WaitForHealthyAsync()
    {
        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        var sampler = CreateSampler(options: null, observers: null, out var signal, out _);

        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));

        var wait = signal.WaitForHealthyAsync(_treeId);
        Assert.That(wait.IsCompleted, Is.False);

        // Drain the partition's in-flight chain so the next tick observes Healthy.
        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            var head = tracker._inFlight.First!.Value;
            tracker.Unlink(head);
        }
        await sampler.SampleOnceAsync(CancellationToken.None);

        await wait.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(wait.IsCompletedSuccessfully, Is.True,
            "the sampler-driven recovery must fan out to every pending awaiter");
    }

    [Test]
    public async Task SampleOnceAsync_dispatch_timeout_only_path_with_no_partition_tracker_fires_Saturated_transition()
    {
        // Coverage for the dispatch-timeout-only classification path:
        // a silo whose only signal is dispatch-timeout trips (no
        // partition tracker registered for the tree) must still be
        // classified as Saturated. Mirrors the production case where
        // a writer-side dispatch trips its deadline against a wedged
        // shard activation but the trip happens before the partition
        // tracker accumulates any pending stamp.
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationDispatchTimeoutThreshold = 2 },
            observers: null,
            out var signal,
            out _);

        // First tick: empty trackers, empty dispatch counts; classifier
        // never runs because perTree.Count == 0. Tree is implicitly
        // Healthy.
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        // Inject the baseline. The sampler initialises _priorInitialised
        // only after the first tick that observes a key, so we need a
        // pre-baseline tick before the trip count crosses the threshold.
        WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 7)] = 0;
        await sampler.SampleOnceAsync(CancellationToken.None);

        // Now trip 2 dispatches in a single window; threshold is 2 so
        // the tree must escalate to Saturated.
        WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 7)] = 2;
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "dispatch-timeout delta crossing threshold must saturate the tree even with no partition tracker");
    }

    [Test]
    public async Task SampleOnceAsync_attributes_dispatch_timeout_transition_to_shard()
    {
        using var capture = new MeterCapture();
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationDispatchTimeoutThreshold = 1 },
            observers: null,
            out _,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        WalCommitLogWriter._dispatchTimeoutCounts[(_treeId, 11)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);

        var sample = capture.For("orleans.lattice.wal.saturation.transitions").Single();
        Assert.That(
            sample.Tags.Any(t => t.Key == LatticeMetrics.TagShard && t.Value is int s && s == 11),
            Is.True,
            "dispatch-timeout-driven transition must be attributed to the affected shard");
    }

    [Test]
    public async Task SampleOnceAsync_fires_recovery_transition_with_previous_state_tag()
    {
        using var capture = new MeterCapture();
        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        var sampler = CreateSampler(options: null, observers: null, out _, out _);

        // Tick 1: Healthy -> Saturated.
        await sampler.SampleOnceAsync(CancellationToken.None);

        // Drain the partition so the next tick recovers.
        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }
        await sampler.SampleOnceAsync(CancellationToken.None);

        var transitions = capture.For("orleans.lattice.wal.saturation.transitions").ToList();
        Assert.That(transitions, Has.Count.EqualTo(2),
            "must fire one transition Healthy->Saturated and one Saturated->Healthy");

        // ConcurrentBag enumeration order is unspecified, so locate the
        // recovery transition by its tag rather than assuming positional
        // order in the capture.
        var recovery = transitions.SingleOrDefault(t =>
            t.Tags.Any(kv => kv.Key == LatticeMetrics.TagWalSaturationState && (string?)kv.Value == "healthy"));
        Assert.That(recovery.Name, Is.Not.Null,
            "must capture a recovery transition tagged state=healthy");
        Assert.That(
            recovery.Tags.Any(t => t.Key == LatticeMetrics.TagWalSaturationPreviousState && (string?)t.Value == "saturated"),
            Is.True,
            "recovery transition must carry the previous state for direction filtering");
    }

    [Test]
    public async Task SampleOnceAsync_dispatches_to_observers_on_recovery_transition()
    {
        var recorder = new RecordingWalSaturationObserver();
        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        var sampler = CreateSampler(
            options: null,
            observers: new[] { (IWalSaturationObserver)recorder },
            out _,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(recorder.Changes, Has.Count.EqualTo(1));

        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(recorder.Changes, Has.Count.EqualTo(2),
            "observers must see the recovery transition, not just the escalation");
        var recovery = recorder.Changes[1];
        Assert.Multiple(() =>
        {
            Assert.That(recovery.PreviousState, Is.EqualTo(WalSaturationState.Saturated));
            Assert.That(recovery.NewState, Is.EqualTo(WalSaturationState.Healthy));
        });
    }

    [Test]
    public void Ctor_throws_on_null_signal()
    {
        var dispatcher = new WalSaturationObserverDispatcher(
            Array.Empty<IWalSaturationObserver>(),
            NullLogger<WalSaturationObserverDispatcher>.Instance);
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        Assert.That(
            () => new WalSaturationSampler(null!, dispatcher, monitor, NullLogger<WalSaturationSampler>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_throws_on_null_dispatcher()
    {
        var signal = new WalSaturationSignal();
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        Assert.That(
            () => new WalSaturationSampler(signal, null!, monitor, NullLogger<WalSaturationSampler>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_throws_on_null_options()
    {
        var signal = new WalSaturationSignal();
        var dispatcher = new WalSaturationObserverDispatcher(
            Array.Empty<IWalSaturationObserver>(),
            NullLogger<WalSaturationObserverDispatcher>.Instance);
        Assert.That(
            () => new WalSaturationSampler(signal, dispatcher, null!, NullLogger<WalSaturationSampler>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_throws_on_null_logger()
    {
        var signal = new WalSaturationSignal();
        var dispatcher = new WalSaturationObserverDispatcher(
            Array.Empty<IWalSaturationObserver>(),
            NullLogger<WalSaturationObserverDispatcher>.Instance);
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        Assert.That(
            () => new WalSaturationSampler(signal, dispatcher, monitor, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task StartAsync_with_infinite_interval_does_not_start_loop()
    {
        // Disabling the sampler via Timeout.InfiniteTimeSpan must leave
        // every tree's signal pinned at Healthy regardless of partition
        // depth - the loop never ticks, so the classifier never runs.
        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationSampleInterval = Timeout.InfiniteTimeSpan },
            observers: null,
            out var signal,
            out _);

        await sampler.StartAsync(CancellationToken.None);
        // Give any (incorrectly-spawned) background loop a window to fire.
        await Task.Delay(50);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "disabled sampler must not classify any tree, regardless of underlying tracker depth");

        await sampler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task StopAsync_is_safe_to_call_when_StartAsync_never_ran()
    {
        var sampler = CreateSampler(options: null, observers: null, out _, out _);
        // No StartAsync call - StopAsync must be a no-op rather than NRE on _loopCts.
        await sampler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task StartAsync_then_StopAsync_cleanly_terminates_the_loop()
    {
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationSampleInterval = TimeSpan.FromMilliseconds(20) },
            observers: null,
            out _,
            out _);

        await sampler.StartAsync(CancellationToken.None);
        await Task.Delay(60); // let several ticks happen
        await sampler.StopAsync(CancellationToken.None);
        // No assertion beyond "did not throw"; the contract under test
        // is that StopAsync settles the background task without leaking
        // a never-completing host-shutdown deadline.
    }

    [Test]
    public void Dispose_is_safe_when_loop_was_never_started()
    {
        var sampler = CreateSampler(options: null, observers: null, out _, out _);
        sampler.Dispose();
    }

    // ---- Recovery-window classifier upgrades --------------------

    /// <summary>
    /// Sampler factory for recovery-window tests that need
    /// deterministic wall-clock control AND a non-zero recovery
    /// window. Uses the internal <see cref="WalSaturationSampler"/>
    /// constructor that accepts a <see cref="TimeProvider"/> so the
    /// recovery-window elapsed-time check is driven by a controllable
    /// clock rather than the system clock. The shared
    /// <see cref="CreateSampler"/> helper deliberately zeroes the
    /// recovery window to preserve the deterministic per-tick
    /// classifier behaviour the sampler shipped with; the
    /// recovery-window tests bypass it.
    /// </summary>
    private (WalSaturationSampler Sampler, WalSaturationSignal Signal, MutableTimeProvider Clock) CreateRecoveryWindowSampler(TimeSpan recoveryWindow)
    {
        var signal = new WalSaturationSignal();
        signal.ResetForTesting();
        var dispatcher = new WalSaturationObserverDispatcher(
            Array.Empty<IWalSaturationObserver>(),
            NullLogger<WalSaturationObserverDispatcher>.Instance);
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeOptions
        {
            WalSaturationRecoveryWindow = recoveryWindow,
        });
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var sampler = new WalSaturationSampler(
            signal,
            dispatcher,
            monitor,
            NullLogger<WalSaturationSampler>.Instance,
            clock);
        return (sampler, signal, clock);
    }

    [Test]
    public async Task SampleOnceAsync_recovery_window_upgrades_Healthy_to_Throttled_within_window()
    {
        // Reproduces the bursty-drain phenotype: the per-partition WAL
        // drain is bursty, so one tick observes a partition at-cap
        // (Saturated) and the very next tick observes every partition
        // with depth=0 (would-be Healthy in the pre-recovery-window
        // classifier). With a non-zero recovery window, the Healthy
        // tick must upgrade to Throttled so the advisory regime
        // persists.
        var (sampler, signal, clock) = CreateRecoveryWindowSampler(TimeSpan.FromSeconds(1));

        // Tick 1: partition 0 at cap => Saturated.
        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "first tick observing at-cap depth must classify Saturated");

        // Drain the partition to mimic the burst end.
        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }

        // Tick 2: depth=0, but only 200 ms have elapsed since the
        // Saturated tick - well inside the 1-second recovery window.
        clock.Advance(TimeSpan.FromMilliseconds(200));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "Healthy classification inside the recovery window must be upgraded to Throttled");
    }

    [Test]
    public async Task SampleOnceAsync_recovery_window_falls_back_to_Healthy_after_window_expires()
    {
        var (sampler, signal, clock) = CreateRecoveryWindowSampler(TimeSpan.FromMilliseconds(500));

        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));

        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }

        // Tick 2: still inside the 500 ms window.
        clock.Advance(TimeSpan.FromMilliseconds(200));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled));

        // Tick 3: total 600 ms elapsed, window expired => fall back to Healthy.
        clock.Advance(TimeSpan.FromMilliseconds(400));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "Healthy classification past the recovery window must NOT be upgraded - the tree has genuinely recovered");
    }

    [Test]
    public async Task SampleOnceAsync_recovery_window_zero_disables_the_upgrade()
    {
        // Zero recovery window is the documented sentinel that
        // restores the deterministic per-tick classifier behaviour
        // the sampler shipped with: the per-tick classification
        // drives the regime directly.
        var (sampler, signal, clock) = CreateRecoveryWindowSampler(TimeSpan.Zero);

        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));

        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }

        // Next tick: depth=0 -> Healthy, no upgrade because window is zero.
        clock.Advance(TimeSpan.FromMilliseconds(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));
    }

    [Test]
    public async Task SampleOnceAsync_recovery_window_infinite_holds_Throttled_forever_after_Saturated()
    {
        // Infinite recovery window: once Saturated has been observed,
        // every subsequent Healthy tick is upgraded to Throttled.
        // Useful for tests / defensive deployments.
        var (sampler, signal, clock) = CreateRecoveryWindowSampler(Timeout.InfiniteTimeSpan);

        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));

        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }

        // Advance way past any reasonable window.
        clock.Advance(TimeSpan.FromHours(1));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "infinite recovery window must hold Throttled regardless of elapsed wall-clock");
    }

    [Test]
    public async Task SampleOnceAsync_recovery_window_per_tree_independent()
    {
        var otherTree = _treeId + "-other";
        var (sampler, signal, clock) = CreateRecoveryWindowSampler(TimeSpan.FromSeconds(1));

        // Tree A reaches Saturated; tree B stays Healthy from the start.
        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        SeedPartition(otherTree, partition: 0, depth: 2, cap: 16);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));
        Assert.That(signal.GetCurrentState(otherTree), Is.EqualTo(WalSaturationState.Healthy),
            "tree B was never at-cap; recovery-window upgrade must not bleed into it");

        var trackerA = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (trackerA._inFlight.Count > 0)
        {
            trackerA.Unlink(trackerA._inFlight.First!.Value);
        }

        clock.Advance(TimeSpan.FromMilliseconds(100));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "tree A is inside its own recovery window");
        Assert.That(signal.GetCurrentState(otherTree), Is.EqualTo(WalSaturationState.Healthy),
            "tree B has never been Saturated; recovery-window upgrade does not apply to it");
    }

    [Test]
    public async Task SampleOnceAsync_recovery_window_refreshes_on_each_Saturated_tick()
    {
        var (sampler, signal, clock) = CreateRecoveryWindowSampler(TimeSpan.FromMilliseconds(500));

        SeedPartition(_treeId, partition: 0, depth: 16, cap: 16);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));

        // Drain so the partition is not at-cap, then re-seed Saturated
        // 300 ms into the original window. The recovery window must
        // restart from the second Saturated tick, not from the first.
        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }
        clock.Advance(TimeSpan.FromMilliseconds(300));
        // Re-seed at-cap depth so the next tick observes Saturated again.
        for (var i = 0; i < 16; i++)
        {
            var pending = new WalCommitLogWriter.PendingAppend(_treeId, partition: 0, entryCount: 1, batchBytes: 0);
            tracker.LinkReturningPreDepth(pending);
        }
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));

        // Drain again and advance 400 ms - past the original 500 ms
        // window but inside the refreshed window from the second
        // Saturated tick (which fired at t+300 ms, so window expires
        // at t+800 ms; we're at t+700 ms).
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }
        clock.Advance(TimeSpan.FromMilliseconds(400));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "the second Saturated tick must refresh the recovery window");
    }

    [Test]
    public async Task SampleOnceAsync_recovery_window_does_not_arm_on_Throttled_only_history()
    {
        // The recovery-window anchor is set ONLY on Saturated
        // observations - never on Throttled. This pins the
        // documented invariant in WalSaturationSampler so a future
        // "improvement" that refreshes on Throttled (which would
        // make sustained moderate load sticky-Throttled forever)
        // is caught immediately. The scenario: a tree that crosses
        // the throttled ratio but never reaches cap, then drains
        // back to Healthy. The Healthy classification must NOT be
        // upgraded because no Saturated tick ever fired.
        var (sampler, signal, clock) = CreateRecoveryWindowSampler(TimeSpan.FromHours(1));

        // Tick 1: depth 12 / cap 16 = 0.75 hits the default
        // throttled ratio exactly without reaching cap.
        SeedPartition(_treeId, partition: 0, depth: 12, cap: 16);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Throttled),
            "depth 12 / cap 16 = 0.75 must classify Throttled (never Saturated)");

        // Drain back to depth 0.
        var tracker = WalCommitLogWriter._trackers[(_treeId, 0)];
        while (tracker._inFlight.Count > 0)
        {
            tracker.Unlink(tracker._inFlight.First!.Value);
        }

        // Tick 2: depth=0 -> Healthy. Despite the 1-hour recovery
        // window, no upgrade fires because the anchor was never
        // set (Throttled does not refresh it).
        clock.Advance(TimeSpan.FromMilliseconds(10));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "Throttled-only history must NOT arm the recovery-window upgrade; only Saturated does");
    }

    [Test]
    public async Task SampleOnceAsync_recovery_window_does_not_upgrade_a_tree_that_has_never_been_Saturated()
    {
        // Pure short-circuit test for the TryGetValue gate: a tree
        // observed only in the Healthy regime from activation onward
        // must stay Healthy regardless of the recovery-window value,
        // because the per-tree anchor dictionary has no entry for it.
        var (sampler, signal, clock) = CreateRecoveryWindowSampler(Timeout.InfiniteTimeSpan);

        // Tick 1: well below the throttled ratio -> Healthy.
        SeedPartition(_treeId, partition: 0, depth: 2, cap: 16);
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy));

        // Tick 2: still Healthy. An infinite recovery window must
        // not cause spurious Throttled upgrades for a tree with no
        // Saturated history - the dictionary lookup short-circuit
        // is the only thing preventing every fresh tree from
        // inheriting a phantom Throttled regime.
        clock.Advance(TimeSpan.FromMinutes(5));
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "infinite recovery window must not upgrade a tree with no Saturated history");
    }

    /// <summary>
    /// Minimal mutable <see cref="TimeProvider"/> for driving the
    /// recovery-window check deterministically without a package
    /// dependency. Mirrors the shape used by
    /// <c>LatticeStorageUsageMetricsTests</c>.
    /// </summary>
    private sealed class MutableTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _now = start;

        public void Advance(TimeSpan by) => _now += by;

        public override DateTimeOffset GetUtcNow() => _now;
    }

    // ----- Provider-failure-rate Saturated branch -----

    [Test]
    public async Task SampleOnceAsync_returns_Saturated_when_provider_failure_delta_crosses_threshold()
    {
        // Healthy depth and zero dispatch-timeout trips so the
        // admission and dispatch-timeout paths stay at Healthy; the
        // provider-failure path is the sole source of the transition.
        // This is the canonical provider-failure-rate regime: the silo's
        // 409-Conflict burst surfaces as provider-failure counter
        // increments well within the dispatch deadline, the admission
        // semaphore never approaches its cap, and the dispatch-timeout
        // counter never trips - yet the silo bleeds entries.
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationProviderFailureRateThreshold = 1 },
            observers: null,
            out var signal,
            out _);

        // First tick: initialises the per-shard prior baseline. No
        // transitions fire because the per-window delta is undefined
        // until the second tick.
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "first tick is provider-failure baseline-only");

        // Simulate one provider failure on shard 5 between ticks (the
        // shape WalCommitLogWriter._providerFailureCounts holds after
        // a downstream IWalShardGrain RPC raises a non-cancellation
        // exception).
        WalCommitLogWriter._providerFailureCounts[(_treeId, 5)] = 1;

        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "a single provider-failure increment in the second tick must cross the default threshold of 1");
    }

    [Test]
    public async Task SampleOnceAsync_provider_failure_delta_is_per_window_not_cumulative()
    {
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationProviderFailureRateThreshold = 1 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // Tick 2: one failure, signal saturates.
        WalCommitLogWriter._providerFailureCounts[(_treeId, 5)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));

        // Tick 3: no new failures, signal recovers to Healthy (depth=0,
        // dispatch trips=0). The delta is the per-window per-shard
        // cumulative-minus-prior, not the cumulative count itself,
        // so a quiescent shard returns to Healthy on the next tick.
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "with no new provider failures and healthy depth, the signal must recover - the delta is per-window, not cumulative");
    }

    [Test]
    public async Task SampleOnceAsync_provider_failure_threshold_zero_disables_branch()
    {
        // Zero is the documented sentinel that disables the provider-
        // failure-rate trigger entirely. With the trigger disabled, a
        // burst of provider failures must NOT raise the tree to
        // Saturated - admission depth stays healthy and the dispatch-
        // timeout counter never trips.
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationProviderFailureRateThreshold = 0 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // A huge per-window delta: still no Saturated transition.
        WalCommitLogWriter._providerFailureCounts[(_treeId, 5)] = 1000;
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "zero threshold must disable the provider-failure-rate branch entirely");
    }

    [Test]
    public async Task SampleOnceAsync_provider_failure_path_independent_of_dispatch_timeout()
    {
        // Both the dispatch-timeout counter and the provider-failure
        // counter are wired into the Saturated branch via OR. The
        // dispatch-timeout counter is at zero; the provider-failure
        // counter alone must drive the regime. Mirrors the per-window-
        // delta-not-cumulative test for the dispatch-timeout branch.
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions
            {
                WalSaturationDispatchTimeoutThreshold = 100, // very high - never crosses
                WalSaturationProviderFailureRateThreshold = 3,
            },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // Trip 2 provider failures - below threshold of 3 - still Healthy.
        WalCommitLogWriter._providerFailureCounts[(_treeId, 5)] = 2;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "2 < 3 provider failures stays Healthy");

        // Trip 3 more in the next window (cumulative 5, delta 3) -
        // crosses the threshold.
        WalCommitLogWriter._providerFailureCounts[(_treeId, 5)] = 5;
        await sampler.SampleOnceAsync(CancellationToken.None);
        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated),
            "per-window delta of 3 must reach the threshold and saturate");
    }

    [Test]
    public async Task SampleOnceAsync_provider_failure_keeps_trees_independent()
    {
        var otherTree = _treeId + "-other";
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        SeedPartition(otherTree, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationProviderFailureRateThreshold = 1 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        // Only the targeted tree's provider failure increments.
        WalCommitLogWriter._providerFailureCounts[(_treeId, 0)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Saturated));
        Assert.That(signal.GetCurrentState(otherTree), Is.EqualTo(WalSaturationState.Healthy),
            "per-tree resolution: a provider-saturated tree must not pollute a peer tree's signal");
    }

    [Test]
    public async Task SampleOnceAsync_attributes_provider_failure_transition_to_shard()
    {
        using var capture = new MeterCapture();
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);
        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationProviderFailureRateThreshold = 1 },
            observers: null,
            out _,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None); // baseline

        WalCommitLogWriter._providerFailureCounts[(_treeId, 9)] = 1;
        await sampler.SampleOnceAsync(CancellationToken.None);

        var sample = capture.For("orleans.lattice.wal.saturation.transitions").Single();
        Assert.That(
            sample.Tags.Any(t => t.Key == LatticeMetrics.TagShard && t.Value is int s && s == 9),
            Is.True,
            "provider-failure-driven transition must be attributed to the affected shard");
    }

    [Test]
    public async Task SampleOnceAsync_provider_failure_first_tick_is_baseline_only()
    {
        using var capture = new MeterCapture();
        SeedPartition(_treeId, partition: 0, depth: 0, cap: 16);

        // Pre-seed a non-zero failure count BEFORE the sampler starts -
        // simulates a silo that crashed mid-burst and reactivated
        // against a static counter that already carries the historical
        // failures. The first sampler tick must initialise the prior
        // baseline at the current count without firing any Saturated
        // transition.
        WalCommitLogWriter._providerFailureCounts[(_treeId, 0)] = 1000;

        var sampler = CreateSampler(
            options: new LatticeOptions { WalSaturationProviderFailureRateThreshold = 1 },
            observers: null,
            out var signal,
            out _);

        await sampler.SampleOnceAsync(CancellationToken.None);

        Assert.That(signal.GetCurrentState(_treeId), Is.EqualTo(WalSaturationState.Healthy),
            "first tick must initialise the baseline at the current count without firing transitions");
        Assert.That(capture.Sum("orleans.lattice.wal.saturation.transitions"), Is.EqualTo(0L),
            "no transition counter must fire on the baseline-init tick");
    }
}
