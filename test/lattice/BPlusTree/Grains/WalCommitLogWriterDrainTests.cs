using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Failing reproducer fixture for the drain-wedge-under-Azure-Tables-account-
/// saturation reliability gap documented in
/// <c>benchmark/azure-throughput/throughput.md</c> section 32.6.
/// <para>
/// The async dump captured during the live wedge pinned 1,556 suspended
/// <see cref="WalCommitLogWriter"/> callers and 739/738
/// <c>PartitionTracker.AcquireAsync</c> frames - callers parked on the
/// writer-side per-partition admission semaphore (cap =
/// <see cref="LatticeOptions.WalMaxPendingBatches"/>) with no recovery
/// path on silo shutdown. <see cref="WalShardGrain"/> has a per-activation
/// drain CTS plus a <see cref="LatticeOptions.WalDrainBudget"/> force-fault
/// (already covered by <c>WalShardGrainTests.DrainBudget.cs</c>), but the
/// writer side does not - the static per-(tree, partition) tracker map
/// outlives every activation and there is no silo-lifetime hook that
/// signals parked callers on shutdown. The result: a caller queued behind
/// a wedged dispatch waits forever, even after the silo has begun draining.
/// </para>
/// <para>
/// These tests fail against the current implementation - they pin the
/// contract that the fix must satisfy (a writer-side drain hook that
/// signals every parked admission-acquire and every in-flight dispatch
/// when the silo begins shutdown). The fix can choose any registration
/// shape (silo lifecycle subject, <see cref="IDisposable"/>,
/// <see cref="IAsyncDisposable"/>); the tests assert behaviour, not API
/// shape.
/// </para>
/// <para>
/// Hermetic isolation: <see cref="WalCommitLogWriter._trackers"/> is a
/// static <see cref="ConcurrentDictionary{TKey,TValue}"/> by design (the
/// stall watchdog walks it from a heap snapshot). Each test calls
/// <see cref="ConcurrentDictionary{TKey,TValue}.Clear"/> in
/// <see cref="SetUp"/> so per-test state cannot bleed across, and uses a
/// unique tree id so cross-test concurrency (NUnit's per-fixture
/// parallelism on CI) cannot share trackers either.
/// </para>
/// </summary>
[TestFixture]
public class WalCommitLogWriterDrainTests
{
    private static int _treeIdSeed;
    private string _treeId = null!;

    [SetUp]
    public void SetUp()
    {
        // Reset the static per-(tree, partition) tracker map so any
        // stale PendingAppend stamps from a prior test do not skew
        // the StallWatchdog's heap walk if it fires during this test.
        // Drain state itself lives on the per-writer instance (not on
        // the tracker), so isolation between tests does not depend on
        // this Clear - it is hygiene only. Each test also uses a
        // unique tree id so cross-test NUnit parallelism cannot share
        // trackers either.
        WalCommitLogWriter._trackers.Clear();
        _treeId = $"tree-drain-{Interlocked.Increment(ref _treeIdSeed)}";
    }

    /// <summary>
    /// Captures every measurement reported on the
    /// <see cref="LatticeMetrics.Meter"/> instrument set for the lifetime
    /// of the test. Local copy of the same shape used in the existing
    /// <c>WalCommitLogWriterDispatchTimeoutTests</c> so this file is
    /// self-contained.
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
            _listener.SetMeasurementEventCallback<double>(
                (inst, value, tags, _) => Records.Add((inst.Name, value, tags.ToArray())));
            _listener.Start();
        }

        public long Count(string instrumentName) =>
            Records.Where(r => r.Name == instrumentName).Sum(r => (long)r.Value);

        public void Dispose() => _listener.Dispose();
    }

    /// <summary>
    /// Builds a writer whose target <see cref="IWalShardGrain"/> is the
    /// supplied substitute. The configured tree id is the per-test
    /// unique id assigned in <see cref="SetUp"/>; mutations produced via
    /// <see cref="MakeMutation"/> carry the same id so the writer
    /// resolves the tracker under the per-test partition key.
    /// </summary>
    private WalCommitLogWriter CreateWriter(
        IWalShardGrain shard,
        LatticeOptions options,
        string clusterId = "site-test")
    {
        ArgumentNullException.ThrowIfNull(options);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns(clusterId);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: options, factory: grainFactory);
        return new WalCommitLogWriter(grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver);
    }

    private WalRecord MakeMutation(string key = "k") => MakeMutationForTree(_treeId, key);

    /// <summary>
    /// Builds a mutation pinned to <paramref name="treeId"/> rather than the
    /// fixture's default <see cref="_treeId"/>. Used exclusively by the AC5
    /// cross-silo isolation test, which seeds disjoint tree-id sets on the
    /// two <see cref="WalCommitLogWriter"/> instances so each writer's
    /// admission tracker is keyed under a distinct <c>(treeId, partition)</c>
    /// pair. Every other test goes through the parameter-less
    /// <see cref="MakeMutation"/> so its tree id is the per-test unique
    /// value assigned in <see cref="SetUp"/>.
    /// </summary>
    private static WalRecord MakeMutationForTree(string treeId, string key) => new()
    {
        TreeId = treeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-test",
    };

    /// <summary>
    /// Drives <paramref name="totalCallers"/> concurrent <see cref="WalCommitLogWriter.AppendAsync"/>
    /// calls so all of them target the <em>same</em> per-(tree, partition) admission
    /// semaphore. The shared partition is achieved by reusing one fixed key
    /// across every record: <see cref="WalCommitLogWriter"/>'s
    /// <c>RouteAsync</c> hashes the key into a partition index, so identical
    /// keys land on the same partition regardless of
    /// <see cref="LatticeOptions.WalPartitions"/>. Without this discipline,
    /// distinct keys would hash to distinct partitions, distinct trackers,
    /// distinct semaphores, and every caller would acquire its own slot
    /// instead of parking on the saturation point the test is trying to
    /// model. The held caller fills the cap (= <see cref="LatticeOptions.WalMaxPendingBatches"/>);
    /// the tail callers are parked inside <c>PartitionTracker.AcquireAsync</c>
    /// - the exact frame the §32.6 async dump pinned at depth 739/738.
    /// Returns the parked-tail tasks so the test can assert behaviour
    /// on them.
    /// </summary>
    private (Task<long> Held, Task<long>[] Parked) PinAdmissionSemaphoreFull(
        WalCommitLogWriter writer,
        TaskCompletionSource<long> heldRelease,
        int totalCallers)
    {
        // The first dispatch fills the only admission slot (cap=1) and
        // hangs forever inside the substitute. The remaining (totalCallers - 1)
        // dispatches are parked behind it inside PartitionTracker.AcquireAsync.
        // Every record uses the same key ("shared-key") so RouteAsync hashes
        // them all to the same partition - otherwise distinct keys would
        // route to distinct partitions and each caller would acquire its
        // own admission slot rather than parking on the saturation point.
        const string SharedKey = "shared-key";
        var held = writer.AppendAsync(MakeMutation(SharedKey));
        var parked = new Task<long>[totalCallers - 1];
        for (var i = 0; i < parked.Length; i++)
        {
            parked[i] = writer.AppendAsync(MakeMutation(SharedKey));
        }
        return (held, parked);
    }

    /// <summary>
    /// AC1 (admission-semaphore drain): callers parked inside
    /// <c>PartitionTracker.AcquireAsync</c> must be released within bounded
    /// time of the silo announcing drain entry. Currently fails: the
    /// writer has no silo-lifetime hook, the static <c>_trackers</c> map
    /// is process-wide and never receives a shutdown signal, so every
    /// parked caller waits forever (only the per-call
    /// <see cref="LatticeOptions.WalAppendDispatchTimeout"/> deadline
    /// releases them - and on the §32.6 reproducer the dispatch timeout
    /// is unbounded for the in-flight dispatch and the wait is the
    /// admission acquire, not the dispatch wait).
    /// </summary>
    [Test]
    public async Task AppendAsync_parked_callers_are_released_within_drain_budget_when_writer_drains()
    {
        var heldRelease = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(heldRelease.Task);

        var drainBudget = TimeSpan.FromMilliseconds(250);
        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalMaxPendingBatches = 1,
            // Long enough that the per-call dispatch deadline cannot
            // release the parked tail - the test must prove the drain
            // hook does. The historical surface released parked callers
            // only via this deadline, but the §32.6 reproducer hits
            // the wedge with the dispatch deadline NOT yet fired (the
            // SDK retry loop is still under its first attempt's
            // deadline), so the wait that bounds the parked callers is
            // the drain hook.
            WalAppendDispatchTimeout = TimeSpan.FromMinutes(5),
            WalDrainBudget = drainBudget,
        });

        var (held, parked) = PinAdmissionSemaphoreFull(writer, heldRelease, totalCallers: 4);

        // Let the held dispatch fill the cap and the tail callers park
        // inside AcquireAsync.
        await Task.Delay(80);
        Assert.That(held.IsCompleted, Is.False, "held dispatch should still be hanging in the substitute");
        Assert.That(parked.All(t => !t.IsCompleted), Is.True, "tail callers should be parked on the admission semaphore");

        // Invoke the writer's drain seam. The drain must release every
        // parked caller within WalDrainBudget + a small grace window;
        // each surfaces a typed TimeoutException naming WalDrainBudget
        // so operators can attribute the trip without source-walking.
        var stopwatch = Stopwatch.StartNew();
        await writer.DrainAsync(CancellationToken.None);
        // Wait for every parked task to settle (with a generous bound
        // so a slow CI worker does not flake the test). The drain itself
        // is synchronous in the writer; the parked WaitAsync continuations
        // surface their TimeoutException on the threadpool the moment the
        // per-tracker drain CTS cancels, which happens inside DrainAsync.
        await Task.WhenAll(parked.Select(t => t.ContinueWith(_ => { }, TaskScheduler.Default))).WaitAsync(drainBudget + TimeSpan.FromSeconds(2));
        stopwatch.Stop();

        Assert.That(stopwatch.Elapsed, Is.LessThan(drainBudget + TimeSpan.FromSeconds(2)),
            "drain must release every parked admission caller within WalDrainBudget + grace");
        foreach (var t in parked)
        {
            Assert.That(async () => await t, Throws.InstanceOf<TimeoutException>(),
                "every parked AcquireAsync caller must surface a typed TimeoutException naming WalDrainBudget so the wedge is attributable");
        }

        heldRelease.TrySetResult(0L);
    }

    /// <summary>
    /// AC2 (admission-semaphore drain attribution): the failure that
    /// releases parked callers on drain must name <c>WalDrainBudget</c>
    /// in its message so operators grepping the silo log can
    /// distinguish the drain-trigger release from the normal
    /// <see cref="LatticeOptions.WalAppendDispatchTimeout"/> release.
    /// Mirrors the assertion the existing
    /// <c>WalShardGrainTests.DrainBudget.cs</c> uses for the shard
    /// grain's force-fault path.
    /// </summary>
    [Test]
    public async Task AppendAsync_parked_callers_surface_TimeoutException_naming_WalDrainBudget()
    {
        var heldRelease = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(heldRelease.Task);

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalMaxPendingBatches = 1,
            WalAppendDispatchTimeout = TimeSpan.FromMinutes(5),
            WalDrainBudget = TimeSpan.FromMilliseconds(200),
        });

        var (held, parked) = PinAdmissionSemaphoreFull(writer, heldRelease, totalCallers: 2);
        await Task.Delay(80);
        Assert.That(parked[0].IsCompleted, Is.False);

        await writer.DrainAsync(CancellationToken.None);
        Assert.That(
            async () => await parked[0],
            Throws.InstanceOf<TimeoutException>()
                .With.Message.Contains(nameof(LatticeOptions.WalDrainBudget)),
            "the parked admission caller's surfaced exception must name WalDrainBudget so operators can grep-attribute the silo-drain release path vs the per-call WalAppendDispatchTimeout release path");

        heldRelease.TrySetResult(0L);
    }

    /// <summary>
    /// AC3 (admission-semaphore drain metric attribution): the writer
    /// must record a counter sample for every drain-triggered release
    /// so a dashboard can graph "how often did we hit this on shutdown"
    /// and so the regression test for the §32.6 wedge has an
    /// instrument-based gate. Tag scheme mirrors
    /// <see cref="LatticeMetrics.WalAppendDispatchTimeouts"/>
    /// (<c>tree</c>, <c>partition</c>) so dashboards can join across
    /// both surfaces. The exact instrument name is the fix's
    /// design choice; this test asserts a name in the
    /// <c>orleans.lattice.wal.append.drain.*</c> namespace exists.
    /// </summary>
    [Test]
    public async Task AppendAsync_drain_release_increments_writer_drain_counter()
    {
        using var capture = new MeterCapture();
        var heldRelease = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(heldRelease.Task);

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalMaxPendingBatches = 1,
            WalAppendDispatchTimeout = TimeSpan.FromMinutes(5),
            WalDrainBudget = TimeSpan.FromMilliseconds(200),
        });

        var (held, parked) = PinAdmissionSemaphoreFull(writer, heldRelease, totalCallers: 3);
        await Task.Delay(80);

        await writer.DrainAsync(CancellationToken.None);
        // Wait for the parked continuations to surface so their metric
        // emissions land before we inspect the capture.
        await Task.WhenAll(parked.Select(t => t.ContinueWith(_ => { }, TaskScheduler.Default))).WaitAsync(TimeSpan.FromSeconds(2));

        var drainReleases = capture.Records
            .Where(r => r.Name.StartsWith("orleans.lattice.wal.writer.append.drain.", StringComparison.Ordinal))
            .ToArray();
        Assert.That(drainReleases, Is.Not.Empty,
            "drain hook must emit at least one drain.* counter sample per released caller for dashboard observability of the writer-admission-semaphore-wedged-at-SIGTERM regime");
        // Each sample carries the (tree, partition) tag pair so a
        // dashboard can attribute the trip to the affected shard.
        foreach (var sample in drainReleases)
        {
            Assert.That(sample.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == _treeId), Is.True,
                "drain.* counter sample must be tagged with the affected tree id");
            Assert.That(sample.Tags.Any(t => t.Key == LatticeMetrics.TagPartition), Is.True,
                "drain.* counter sample must be tagged with the affected writer partition");
        }

        heldRelease.TrySetResult(0L);
    }

    /// <summary>
    /// AC4 (admission-semaphore drain re-entry): after a drain completes,
    /// the writer must reject new <see cref="WalCommitLogWriter.AppendAsync"/>
    /// calls immediately rather than re-entering the admission queue
    /// (the silo is shutting down; a fresh caller blocked on a
    /// drained semaphore would never make progress and would extend
    /// the shutdown grace window past the drain budget). Currently
    /// fails: there is no drain state on the writer at all, so a new
    /// append after drain re-enters the queue and behaves identically
    /// to a pre-drain append.
    /// </summary>
    [Test]
    public async Task AppendAsync_after_drain_fails_fast_instead_of_re_entering_admission_queue()
    {
        var heldRelease = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(heldRelease.Task);

        var writer = CreateWriter(shard, new LatticeOptions
        {
            WalMaxPendingBatches = 1,
            WalAppendDispatchTimeout = TimeSpan.FromMinutes(5),
            WalDrainBudget = TimeSpan.FromMilliseconds(150),
        });

        // Fill the cap, then drain the writer. Use one shared key so both
        // the held and post-drain callers route to the same partition;
        // distinct keys would hash to distinct partitions and the
        // post-drain caller would acquire its own admission slot rather
        // than meeting the writer-level drain gate we are pinning here.
        const string SharedKey = "shared-key";
        var held = writer.AppendAsync(MakeMutation(SharedKey));
        await Task.Delay(40);
        Assert.That(held.IsCompleted, Is.False);

        await writer.DrainAsync(CancellationToken.None);
        // Post-drain appends must fail fast at the writer-level gate
        // (InvalidOperationException from GetTracker) rather than
        // blocking on the (still-healthy) shared admission gate that
        // would never release them on this writer's drain token. The
        // gate is on the writer instance, not on the tracker, so
        // successor writers in the same process see a clean gate and
        // can keep dispatching - that is the multi-silo correctness
        // property this fix pins.
        var postDrain = writer.AppendAsync(MakeMutation(SharedKey));
        Assert.That(
            async () => await postDrain.WaitAsync(TimeSpan.FromMilliseconds(500)),
            Throws.InstanceOf<InvalidOperationException>().Or.InstanceOf<TimeoutException>(),
            "post-drain appends must not re-enter the admission queue; they must fail fast with a typed exception so the caller's foreground commit path treats the silo as draining rather than retrying");

        heldRelease.TrySetResult(0L);
    }

    /// <summary>
    /// AC5 (cross-writer / multi-silo isolation): draining one
    /// <see cref="WalCommitLogWriter"/> instance must release only that
    /// instance's parked admission callers. A second writer instance,
    /// representing a peer silo in the cluster, must observe no change
    /// to its own parked callers, in-flight dispatches, or admission
    /// state.
    /// <para>
    /// Why this matters: production silos are separate processes with
    /// disjoint <see cref="WalCommitLogWriter._trackers"/> statics, so
    /// the cross-silo property is automatic at the process boundary. In
    /// the same process (this test), the static is shared, so the
    /// property has to come from <em>scoping</em> the drain to the keys
    /// that belong to the draining writer's traffic. The test seeds
    /// disjoint tree-id sets on each writer (silo A: <c>silo-a-tree-N</c>,
    /// silo B: <c>silo-b-tree-N</c>) so each writer's traffic lands in
    /// trackers keyed under its own tree ids; a correct drain releases
    /// only the draining writer's tree-id keys and leaves the peer's
    /// tracker entries untouched.
    /// </para>
    /// <para>
    /// A failing implementation - one that iterated every entry in the
    /// shared <c>_trackers</c> map regardless of which writer owns each
    /// tracker's traffic - would also release silo B's parked caller,
    /// silently breaking the per-silo scoping contract that production
    /// relies on for rolling restarts. This test pins the boundary.
    /// </para>
    /// </summary>
    [Test]
    public async Task DrainAsync_releases_only_the_draining_writer_callers_leaving_a_peer_writer_untouched()
    {
        // Build two writer instances, each with its own pair of (shard substitute,
        // held-release TCS). The substitutes hang forever inside the shard RPC so
        // every admission slot we acquire is genuinely held, and every subsequent
        // caller is parked inside PartitionTracker.AcquireAsync.
        var heldReleaseA = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var heldReleaseB = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        var shardA = Substitute.For<IWalShardGrain>();
        shardA.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(heldReleaseA.Task);

        var shardB = Substitute.For<IWalShardGrain>();
        shardB.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(heldReleaseB.Task);

        var options = new LatticeOptions
        {
            WalMaxPendingBatches = 1,
            WalAppendDispatchTimeout = TimeSpan.FromMinutes(5),
            WalDrainBudget = TimeSpan.FromMilliseconds(250),
        };
        var writerA = CreateWriter(shardA, options);
        var writerB = CreateWriter(shardB, options);

        // Disjoint tree-id sets so each writer's traffic lands in trackers
        // keyed under its own ids. The peer writer's tracker entries live
        // under different (treeId, partition) keys, so a correctly-scoped
        // drain on writer A must not touch them.
        var siloAtree = $"silo-a-{_treeId}";
        var siloBtree = $"silo-b-{_treeId}";

        // Seed: 1 held + 1 parked on each writer. Within each writer the
        // two callers share a single key so they hash to the same
        // partition - otherwise the second caller would acquire its own
        // admission slot instead of parking on the saturation point.
        const string SharedKey = "shared-key";
        var heldA = writerA.AppendAsync(MakeMutationForTree(siloAtree, SharedKey));
        var parkedA = writerA.AppendAsync(MakeMutationForTree(siloAtree, SharedKey));
        var heldB = writerB.AppendAsync(MakeMutationForTree(siloBtree, SharedKey));
        var parkedB = writerB.AppendAsync(MakeMutationForTree(siloBtree, SharedKey));

        // Let the held dispatches fill each cap and the parked callers park.
        await Task.Delay(80);
        Assert.Multiple(() =>
        {
            Assert.That(heldA.IsCompleted, Is.False, "writer A held dispatch should still be hanging");
            Assert.That(parkedA.IsCompleted, Is.False, "writer A parked caller should be on the admission semaphore");
            Assert.That(heldB.IsCompleted, Is.False, "writer B held dispatch should still be hanging");
            Assert.That(parkedB.IsCompleted, Is.False, "writer B parked caller should be on the admission semaphore");
        });

        await writerA.DrainAsync(CancellationToken.None);

        // Writer A's parked caller surfaces TimeoutException naming WalDrainBudget.
        Assert.That(async () => await parkedA,
            Throws.InstanceOf<TimeoutException>().With.Message.Contains(nameof(LatticeOptions.WalDrainBudget)),
            "writer A's parked caller must surface a typed TimeoutException naming WalDrainBudget after writer A drains");

        // Writer B's parked caller is untouched by writer A's drain - it is
        // still parked on its own admission semaphore (whose held dispatch
        // is still hanging in shardB's substitute). The cross-writer drain
        // signal did not leak through the shared static _trackers map; the
        // per-instance _ownedTrackers scoping kept silo A's drain off
        // silo B's trackers.
        var bSettled = await Task.WhenAny(parkedB, Task.Delay(200));
        Assert.That(bSettled, Is.Not.SameAs(parkedB),
            "writer B's parked caller must remain parked after writer A drains; a release here means the drain scope leaked across writer instances (would break per-silo rolling-restart semantics in production)");

        // Drain writer B at end of test so the parked caller and held
        // dispatch unwind cleanly before the fixture tears down.
        await writerB.DrainAsync(CancellationToken.None);
        Assert.That(async () => await parkedB,
            Throws.InstanceOf<TimeoutException>().With.Message.Contains(nameof(LatticeOptions.WalDrainBudget)));

        // Release the substitute-held tasks so the abandoned shard-RPC tasks
        // settle and GC can reclaim them, instead of leaking into the test
        // runner's freachable queue past the fixture lifetime.
        heldReleaseA.TrySetResult(0L);
        heldReleaseB.TrySetResult(0L);
    }
}
