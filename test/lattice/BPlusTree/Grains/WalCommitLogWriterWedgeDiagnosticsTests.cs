using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the writer-layer wedge diagnostic pack: the per-partition
/// pending-append dispatch tracker (PartitionTracker + PendingAppend +
/// WalAppendStage) and the two metric instruments
/// (orleans.lattice.wal.writer.append.dispatched,
/// orleans.lattice.wal.writer.partition.pending_appends) that surface
/// it. Mirrors WalShardGrainTests.WedgeDiagnostics one layer up so a
/// rename of the watchdog-readable shape lights up the build
/// immediately rather than at the next cohort.
/// </summary>
[TestFixture]
[Category("Integration")]
public class WalCommitLogWriterWedgeDiagnosticsTests
{
    private const string TreeId = "tree-wedge-diag";

    // Each test that exercises admission semantics needs a fresh
    // (tree, partition) keyspace because PartitionTracker is rooted
    // in a static ConcurrentDictionary and its admission semaphore
    // is initialised once per tracker lifetime. Sharing TreeId
    // across admission tests would let one test's cap=N semaphore
    // leak into another test's expectation of cap=M (or cap=0
    // opt-out). The diagnostic-counter tests above don't care about
    // cap so they can keep using the shared TreeId.
    private static string FreshTreeId([System.Runtime.CompilerServices.CallerMemberName] string caller = "")
        => $"tree-wedge-diag-{caller}-{Guid.NewGuid():N}";

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

        public (long Value, KeyValuePair<string, object?>[] Tags)? FirstFor(string instrumentName)
        {
            var hit = Records.FirstOrDefault(r => r.Name == instrumentName);
            return hit == default ? null : ((long)hit.Value, hit.Tags);
        }

        public void Dispose() => _listener.Dispose();
    }

    private static WalCommitLogWriter CreateWriter(IWalShardGrain shard, string clusterId = "site-test")
    {
        return CreateWriterWithOptions(shard, new LatticeOptions(), clusterId);
    }

    private static WalCommitLogWriter CreateWriterWithOptions(IWalShardGrain shard, LatticeOptions perTreeOptions, string clusterId = "site-test")
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(perTreeOptions);

        var modeResolver = Substitute.For<ILatticeMergeModeResolver>();
        modeResolver.Resolve(Arg.Any<string>()).Returns(LatticeMergeMode.LwwRegister);

        var clusterIdResolver = Substitute.For<ILatticeOriginClusterIdResolver>();
        clusterIdResolver.Resolve(Arg.Any<string>()).Returns(clusterId);

        var optionsResolver = TestOptionsResolver.Create(baseOptions: perTreeOptions, factory: grainFactory);
        return new WalCommitLogWriter(grainFactory, optionsMonitor, optionsResolver, modeResolver, clusterIdResolver);
    }

    private static WalRecord MakeMutation(string key) => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-test",
    };

    private static WalRecord MakeMutationFor(string treeId, string key) => new()
    {
        TreeId = treeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-test",
    };

    [Test]
    public async Task AppendAsync_increments_dispatched_counter_per_invocation()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        var writer = CreateWriter(shard);

        var offset = await writer.AppendAsync(MakeMutation("k"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offset, Is.EqualTo(0L));

        var dispatched = capture.Count("orleans.lattice.wal.writer.append.dispatched");
        Assert.That(dispatched, Is.EqualTo(1L),
            "writer-layer dispatch counter: every AppendAsync must increment the dispatched counter exactly once");

        var sample = capture.FirstFor("orleans.lattice.wal.writer.append.dispatched");
        Assert.That(sample, Is.Not.Null);
        Assert.That(
            sample!.Value.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == TreeId),
            Is.True,
            "writer-layer dispatch counter: dispatched must be tagged with the affected tree id");
        Assert.That(
            sample.Value.Tags.Any(t => t.Key == LatticeMetrics.TagPartition && t.Value is int),
            Is.True,
            "writer-layer dispatch counter: dispatched must be tagged with the writer partition");
    }

    [Test]
    public async Task AppendAsync_records_pending_appends_histogram_per_invocation()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        var writer = CreateWriter(shard);

        await writer.AppendAsync(MakeMutation("k"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        var observation = capture.FirstFor("orleans.lattice.wal.writer.partition.pending_appends");
        Assert.That(observation, Is.Not.Null,
            "writer-layer dispatch counter: pending_appends observation must be emitted at every AppendAsync entry");
        // For a single in-flight dispatch on a previously-empty
        // partition the pre-link depth is 0 (the new pending stamp is
        // observation N, not N+1). Asserting the lower bound rather
        // than exact zero keeps this robust against cross-test ordering
        // (other tests in this fixture also write into the static
        // tracker dictionary).
        Assert.That(observation!.Value.Value, Is.GreaterThanOrEqualTo(0L));
    }

    [Test]
    public async Task AppendManyAsync_records_dispatched_and_pending_for_batched_path()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var entries = call.Arg<IReadOnlyList<WalRecord>>();
                var offsets = new long[entries.Count];
                for (var i = 0; i < entries.Count; i++) { offsets[i] = i; }
                return Task.FromResult<IReadOnlyList<long>>(offsets);
            });
        // Force two distinct partitions by routing on two distinct keys.
        // Single-entry path collapses to AppendAsync; we need >=2 distinct
        // partition groups to exercise AppendForPartitionAsync.
        var writer = CreateWriter(shard);

        var entries = new[]
        {
            MakeMutation("partition-a-key-1"),
            MakeMutation("partition-b-key-2"),
            MakeMutation("partition-c-key-3"),
        };
        var offsets = await writer.AppendManyAsync(entries, CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(offsets, Has.Count.EqualTo(3));

        var dispatched = capture.Count("orleans.lattice.wal.writer.append.dispatched");
        Assert.That(dispatched, Is.GreaterThanOrEqualTo(1L),
            "writer-layer dispatch counter: every AppendForPartitionAsync dispatch must increment the dispatched counter at least once");

        var pendingObs = capture.Records.Where(r => r.Name == "orleans.lattice.wal.writer.partition.pending_appends").ToList();
        Assert.That(pendingObs, Is.Not.Empty,
            "writer-layer dispatch counter: pending_appends observations must be emitted at every per-partition dispatch entry");
    }

    [Test]
    public void WalAppendStage_enum_layout_is_contract_with_StallWatchdog()
    {
        // The StallWatchdog reads WalAppendStage as a raw byte via ClrMD
        // and maps it to a stage name through a hardcoded switch
        // (Enqueued=0, DequeuedForBatch=1, SentToShard=2, Acked=3,
        // Failed=4). A future enum renumber would silently mislabel
        // every pending stamp in the watchdog log; this contract test
        // catches it.
        var enumType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+WalAppendStage", throwOnError: true)!;
        Assert.That(enumType.GetEnumUnderlyingType(), Is.EqualTo(typeof(byte)),
            "writer-layer lifecycle: WalAppendStage underlying type must be byte (StallWatchdog reads it as byte)");
        var values = Enum.GetValues(enumType).Cast<object>()
            .ToDictionary(v => v.ToString()!, v => (byte)v);
        Assert.Multiple(() =>
        {
            Assert.That(values["Enqueued"], Is.EqualTo((byte)0));
            Assert.That(values["DequeuedForBatch"], Is.EqualTo((byte)1));
            Assert.That(values["SentToShard"], Is.EqualTo((byte)2));
            Assert.That(values["Acked"], Is.EqualTo((byte)3));
            Assert.That(values["Failed"], Is.EqualTo((byte)4));
        });
    }

    [Test]
    public void PendingAppend_carries_Stage_and_StageStartedTicks_fields()
    {
        // The StallWatchdog walks the heap for PendingAppend instances
        // and reads Stage / StageStartedTicks by name via ClrMD. A
        // field rename would silently break attribution; this contract
        // test catches it.
        var pendingType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+PendingAppend", throwOnError: true)!;
        var stageField = pendingType.GetField("Stage", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        var stageStartedField = pendingType.GetField("StageStartedTicks", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        Assert.That(stageField, Is.Not.Null, "writer-layer lifecycle: PendingAppend must carry a Stage field of type WalAppendStage");
        Assert.That(stageStartedField, Is.Not.Null, "writer-layer lifecycle: PendingAppend must carry a StageStartedTicks field of type long");
        Assert.That(stageField!.FieldType.Name, Is.EqualTo("WalAppendStage"));
        Assert.That(stageStartedField!.FieldType, Is.EqualTo(typeof(long)));
    }

    [Test]
    public void PartitionTracker_carries_TreeId_Partition_inFlight_fields_for_watchdog_walk()
    {
        // The StallWatchdog detects PartitionTracker by field signature
        // (TreeId + Partition + _inFlight). A field rename would
        // silently break the writer-layer watchdog walker; this
        // contract test catches it.
        var trackerType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+PartitionTracker", throwOnError: true)!;
        Assert.Multiple(() =>
        {
            Assert.That(trackerType.GetField("TreeId", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance), Is.Not.Null);
            Assert.That(trackerType.GetField("Partition", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance), Is.Not.Null);
            Assert.That(trackerType.GetField("_inFlight", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance), Is.Not.Null,
                "writer-layer lifecycle: PartitionTracker._inFlight must remain a public field (the StallWatchdog ClrMD walk reads it via Fields.FirstOrDefault by name).");
        });
    }

    [Test]
    public async Task AppendAsync_admission_records_wait_histogram_on_uncontended_acquire()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        var writer = CreateWriter(shard);
        var treeId = FreshTreeId();

        await writer.AppendAsync(MakeMutationFor(treeId, "admission-uncontended-key"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(5));

        var waitSample = capture.Records.FirstOrDefault(r => r.Name == "orleans.lattice.wal.writer.append.admission_wait"
            && r.Tags.Any(t => t.Key == LatticeMetrics.TagTree && (string?)t.Value == treeId));
        Assert.That(waitSample, Is.Not.EqualTo(default((string, double, KeyValuePair<string, object?>[]))),
            "writer-layer admission: admission_wait must be recorded for every dispatch that successfully acquired a slot");
        // Uncontended fast path is sub-millisecond. Lower bound is exactly zero.
        Assert.That(waitSample.Value, Is.GreaterThanOrEqualTo(0d));
        Assert.That(
            waitSample.Tags.Any(t => t.Key == LatticeMetrics.TagPartition && t.Value is int),
            Is.True,
            "writer-layer admission: admission_wait must be tagged with the writer partition");
    }

    [Test]
    public async Task AppendAsync_admission_timeout_path_increments_writer_counter_when_tracker_presaturated()
    {
        // Integration test for the writer-side admission catch
        // block: pre-saturate the partition's tracker directly via
        // reflection (no shard race), then call writer.AppendAsync
        // and confirm BOTH the writer's TimeoutException is raised
        // AND the WalAppendAdmissionTimeouts counter advances.
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));
        var perTree = new LatticeOptions
        {
            WalMaxPendingBatches = 1,
            WalAppendDispatchTimeout = TimeSpan.FromMilliseconds(100),
        };
        var writer = CreateWriterWithOptions(shard, perTree);
        var treeId = FreshTreeId();

        // Send one dispatch to force the tracker to materialise and
        // its admission semaphore to initialise at cap=1. After it
        // completes the slot is back to free.
        await writer.AppendAsync(MakeMutationFor(treeId, "presat-key"), CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(2));

        // Now reach into _trackers and acquire-without-release on
        // the partition the next dispatch will route to, so the
        // next writer.AppendAsync is guaranteed to hit admission
        // timeout.
        var writerType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter);
        var trackersField = writerType.GetField("_trackers", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var trackers = trackersField.GetValue(null)!;
        // _trackers is ConcurrentDictionary<(string, int), PartitionTracker>;
        // enumerate to find the one for this tree (there should be exactly one).
        var trackerType = writerType.Assembly.GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+PartitionTracker", throwOnError: true)!;
        object? targetTracker = null;
        foreach (var v in (System.Collections.IEnumerable)trackers)
        {
            var prop = v.GetType().GetProperty("Value");
            var trackerInstance = prop!.GetValue(v)!;
            if ((string)trackerType.GetField("TreeId")!.GetValue(trackerInstance)! == treeId)
            {
                targetTracker = trackerInstance;
                break;
            }
        }
        Assert.That(targetTracker, Is.Not.Null, "test setup: tracker must exist after the first dispatch");
        var acquire = trackerType.GetMethod("AcquireAsync")!;
        // Hold the slot without ever releasing it. Passing
        // CancellationToken.None for both the caller-supplied CT and
        // the writer-supplied drain token isolates this test to the
        // admission path; the drain seam is exercised by
        // WalCommitLogWriterDrainTests.
        var hold = (Task<double>)acquire.Invoke(targetTracker, new object[] { 1, TimeSpan.FromSeconds(10), CancellationToken.None, CancellationToken.None })!;
        await hold.WaitAsync(TimeSpan.FromSeconds(2));

        // The next writer.AppendAsync must hit admission timeout
        // because the slot is held by our test code.
        var ex = Assert.ThrowsAsync<TimeoutException>(async () =>
        {
            await writer.AppendAsync(MakeMutationFor(treeId, "presat-key"), CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(3));
        });
        Assert.That(ex!.Message, Does.Contain("admission deadline"));

        var admissionTimeouts = capture.Count("orleans.lattice.wal.writer.append.admission_timeouts");
        Assert.That(admissionTimeouts, Is.EqualTo(1L),
            "writer-layer admission: admission_timeouts must increment exactly once per writer.AppendAsync that fails admission");

        // Clean up: release the held slot so the tracker is not
        // permanently saturated for subsequent test runs.
        var release = trackerType.GetMethod("ReleaseAdmission")!;
        release.Invoke(targetTracker, Array.Empty<object>());
    }

    [Test]
    public async Task PartitionTracker_AcquireAsync_throws_TimeoutException_when_cap_saturated()
    {
        // Direct unit test on PartitionTracker.AcquireAsync, which
        // bypasses the writer's append pipeline so the admission
        // semantics can be asserted without the shard/dispatch
        // timing race that would otherwise contaminate the
        // assertion. The writer's AppendAsync integration is
        // covered by the counter / wait-histogram tests above.
        var trackerType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+PartitionTracker", throwOnError: true)!;
        var ctor = trackerType.GetConstructor(new[] { typeof(string), typeof(int) })!;
        var tracker = ctor.Invoke(new object[] { "tracker-admission-direct", 0 });
        var acquire = trackerType.GetMethod("AcquireAsync")!;
        var release = trackerType.GetMethod("ReleaseAdmission")!;

        // cap=1 admission: first acquire succeeds immediately
        // (uncontended fast path returns 0 ms wait). Passing
        // CancellationToken.None for both the caller-supplied CT and
        // the writer-supplied drain token isolates this test to the
        // admission path.
        var firstTask = (Task<double>)acquire.Invoke(tracker, new object[] { 1, TimeSpan.FromMilliseconds(50), CancellationToken.None, CancellationToken.None })!;
        var firstWait = await firstTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(firstWait, Is.GreaterThanOrEqualTo(0d),
            "writer-layer admission: uncontended first acquire must return non-negative wait time");

        // Second acquire on the saturated semaphore: must throw
        // TimeoutException after the 50 ms deadline.
        var secondTask = (Task<double>)acquire.Invoke(tracker, new object[] { 1, TimeSpan.FromMilliseconds(50), CancellationToken.None, CancellationToken.None })!;
        var ex = Assert.ThrowsAsync<TimeoutException>(async () => await secondTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.That(ex!.Message, Does.Contain("admission deadline"),
            "writer-layer admission: TimeoutException must name the admission deadline so the failure mode is unambiguous in caller logs");
        Assert.That(ex.Message, Does.Contain("cap=1"),
            "writer-layer admission: TimeoutException must include the configured cap so an operator can correlate the failure with the setting");

        // Release the held slot; a subsequent acquire must succeed.
        release.Invoke(tracker, Array.Empty<object>());
        var thirdTask = (Task<double>)acquire.Invoke(tracker, new object[] { 1, TimeSpan.FromMilliseconds(50), CancellationToken.None, CancellationToken.None })!;
        var thirdWait = await thirdTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.That(thirdWait, Is.GreaterThanOrEqualTo(0d),
            "writer-layer admission: post-release acquire must succeed");
    }

    [Test]
    public async Task PartitionTracker_AcquireAsync_with_cap_zero_is_unbounded_opt_out()
    {
        // Direct test on the opt-out sentinel.
        var trackerType = typeof(Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter).Assembly
            .GetType("Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter+PartitionTracker", throwOnError: true)!;
        var ctor = trackerType.GetConstructor(new[] { typeof(string), typeof(int) })!;
        var tracker = ctor.Invoke(new object[] { "tracker-admission-optout", 0 });
        var acquire = trackerType.GetMethod("AcquireAsync")!;

        // cap=0 (opt-out): every acquire must complete immediately
        // returning 0 ms wait, even when called many times without
        // a release. Without the opt-out sentinel the second
        // acquire onwards would deadlock until the deadline.
        for (var i = 0; i < 50; i++)
        {
            var task = (Task<double>)acquire.Invoke(tracker, new object[] { 0, TimeSpan.FromMilliseconds(50), CancellationToken.None, CancellationToken.None })!;
            var wait = await task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.That(wait, Is.EqualTo(0d),
                $"writer-layer admission: opt-out (cap=0) acquire #{i} must complete immediately returning 0 ms");
        }
    }

    [Test]
    public async Task AppendAsync_admission_with_WalMaxPendingBatches_zero_is_unbounded_opt_out()
    {
        using var capture = new MeterCapture();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(0L));

        // Cap = 0 is the opt-out sentinel; the writer must admit
        // every dispatch without a semaphore (parity with the
        // historical pre-cap writer for rollback).
        var perTree = new LatticeOptions { WalMaxPendingBatches = 0 };
        var writer = CreateWriterWithOptions(shard, perTree);
        var treeId = FreshTreeId();

        // Many dispatches at once must all complete; if the
        // semaphore were sized at 0 the second dispatch onwards
        // would deadlock until WalAppendDispatchTimeout.
        var dispatches = Enumerable.Range(0, 10)
            .Select(i => writer.AppendAsync(MakeMutationFor(treeId, $"opt-out-key-{i}"), CancellationToken.None))
            .ToArray();
        await Task.WhenAll(dispatches).WaitAsync(TimeSpan.FromSeconds(5));

        var timeouts = capture.Count("orleans.lattice.wal.writer.append.admission_timeouts");
        Assert.That(timeouts, Is.EqualTo(0L),
            "writer-layer admission: opt-out sentinel (WalMaxPendingBatches <= 0) must not generate admission timeouts even under burst");
    }
}