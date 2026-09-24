using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3393: on a graceful deactivation the durable pin for the FINAL
/// checkpoint advance must land even when the deactivation deadline tears the
/// activation down before the trailing barriers run.
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect.</b> The recorded drain spent its deactivation deadline on the
/// upward digest publish, which led the hook. By the time the durability
/// barriers ran the activation was invalid, so <c>snapshot_capture</c> and
/// <c>frontier_pin</c> both threw "Attempt to access an invalid activation" and
/// the final advance's pin was never published. The leaf went dormant holding
/// the shared WAL's trim floor at an older checkpoint.
/// </para>
/// <para>
/// <b>The fix, and the fixture per change.</b> (1) The teardown persist's tail
/// publishes the pin through the awaited batched flush as its first step,
/// coverage-gated from the PERSISTED checkpoint (#3476). (2) The two trailing
/// barriers skip, counted under their existing reason, instead of throwing on
/// a torn-down activation; the catch is narrow. (3) Durability work runs before
/// the digest publish. Each test below names the change it pins and goes red
/// when that change is removed.
/// </para>
/// <para>
/// <b>Teardown model.</b> A real teardown invalidates the activation, after
/// which every grain-state read throws <see cref="InvalidOperationException"/>
/// from <c>GrainRuntime.CheckRuntimeContext</c>, and Orleans cancels the
/// deactivation token. The fixtures reproduce both halves at one instant: the
/// token is cancelled and <see cref="FakePersistentState{T}.ThrowOnStateAccess"/>
/// is armed together, from inside the step the deadline expires during.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    private const string FinalAdvanceTreeId = "tree-3393-final-advance";

    private const string InvalidActivationMessage = "Attempt to access an invalid activation";

    /// <summary>How a durable pin reached the reporter.</summary>
    private enum FinalAdvancePinChannel
    {
        /// <summary>The awaited batched flush: a durable write the caller waits for.</summary>
        Batched,

        /// <summary>The debounced fire-and-forget mirror.</summary>
        Mirror,
    }

    /// <summary>One pin publication and the leaf state it was published against.</summary>
    private readonly record struct FinalAdvancePin(
        FinalAdvancePinChannel Channel,
        long PublishedOffset,
        long PersistedCheckpoint,
        long Coverage);

    /// <summary>
    /// A leaf with a coalesced checkpoint persist, durable snapshot coverage at
    /// 0, a parent, and hooks into each step a deactivation deadline can
    /// expire during.
    /// </summary>
    private sealed class FinalAdvanceLeaf
    {
        public BPlusLeafGrain Grain = null!;
        public FakePersistentState<LeafNodeState> State = null!;
        public ILeafCursorReporter Reporter = null!;
        public ILeafSnapshotStorageGrain Snapshot = null!;
        public readonly List<FinalAdvancePin> Published = [];
        public readonly List<string> Calls = [];
        public readonly List<string> Warnings = [];

        /// <summary>Runs inside the per-partition cursor report.</summary>
        public Action? OnCursorReport;

        /// <summary>Runs inside the parent's digest hop.</summary>
        public Action? OnParentDigest;

        public IEnumerable<FinalAdvancePin> Batched =>
            Published.Where(p => p.Channel == FinalAdvancePinChannel.Batched);

        public IEnumerable<FinalAdvancePin> Mirrored =>
            Published.Where(p => p.Channel == FinalAdvancePinChannel.Mirror);

        /// <summary>Makes the deadline expire now: cancel the token and invalidate the activation.</summary>
        public void TearDown(CancellationTokenSource deadline)
        {
            deadline.Cancel();
            State.ThrowOnStateAccess = new InvalidOperationException(InvalidActivationMessage);
        }
    }

    /// <summary>Records every warning the leaf logs.</summary>
    private sealed class FinalAdvanceLoggerFactory(List<string> warnings) : ILoggerFactory
    {
        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new Recorder(warnings);

        public void Dispose()
        {
        }

        private sealed class Recorder(List<string> warnings) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (logLevel == LogLevel.Warning)
                {
                    warnings.Add(formatter(state, exception));
                }
            }
        }
    }

    private static FinalAdvanceLeaf CreateFinalAdvanceLeaf(
        ILeafReplayCoordinatorGrain coordinator,
        int digestCoalescingWindowMs)
    {
        var leaf = new FinalAdvanceLeaf();

        var snapshotStub = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(new LeafSnapshotBlob
            {
                SnapshotOffset = 0L,
                Rows = [],
                CapturedAtTicks = 1L,
                SnapshotOffsetsByPartition = [0L],
            }));
        snapshotStub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                leaf.Calls.Add("capture");
                return Task.FromResult(LeafSnapshotSaveOutcome.Kept);
            });

        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = FinalAdvanceTreeId;
        state.State.ParentId = GrainId.Create("internal", "tree-3393-final-advance-parent");
        state.State.ProjectionCheckpointOffset = -1L;

        void Record(FinalAdvancePinChannel channel, long offset) => leaf.Published.Add(new FinalAdvancePin(
            channel,
            offset,
            state.State.ProjectionCheckpointOffset,
            leaf.Grain.DurableSnapshotCoverageForPartition(0)));

        var reporter = Substitute.For<ILeafCursorReporter>();
        reporter.FlushDurableMaterialiserFrontierAsync(
                Arg.Any<string>(),
                Arg.Any<IReadOnlyList<MaterialiserPinReport>>(),
                Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                foreach (var report in call.ArgAt<IReadOnlyList<MaterialiserPinReport>>(1))
                {
                    Record(FinalAdvancePinChannel.Batched, report.CheckpointOffset);
                    leaf.Calls.Add($"pin:{report.CheckpointOffset}");
                }

                return Task.CompletedTask;
            });
        reporter
            .When(r => r.NoteDurableMaterialiserFrontier(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<long>()))
            .Do(call => Record(FinalAdvancePinChannel.Mirror, call.ArgAt<long>(3)));
        reporter.ReportAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                leaf.Calls.Add("cursor");
                leaf.OnCursorReport?.Invoke();
                return Task.CompletedTask;
            });

        var parent = Substitute.For<IBPlusInternalGrain>();
        parent.OnChildDigestPublishedAsync(Arg.Any<GrainId>(), Arg.Any<ChildDigestSnapshot>())
            .Returns(_ =>
            {
                leaf.Calls.Add("digest");
                leaf.OnParentDigest?.Invoke();
                return Task.CompletedTask;
            });

        var sc = new ServiceCollection();
        sc.AddSingleton(Substitute.For<ICommitLogReader>());
        sc.AddSingleton(reporter);
        sc.AddSingleton<ILoggerFactory>(new FinalAdvanceLoggerFactory(leaf.Warnings));
        var services = sc.BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", Guid.NewGuid().ToString("N")));
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        grainFactory.GetGrain<ILeafSnapshotStorageGrain>(Arg.Any<Guid>()).Returns(snapshotStub);
        grainFactory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(parent);

        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions
            {
                // Coalesced persist: a replayed advance stays PENDING until the
                // teardown persist commits it, which is the production shape.
                MaterialiserCheckpointInterval = TimeSpan.FromHours(1),
                MaterialiserCheckpointEntries = 1_000_000,
                WalPartitions = 1,
                LeafSnapshotReClassifyEveryNCheckpoints = 1000,
                DigestCoalescingWindowMs = digestCoalescingWindowMs,
                MaintainProjectionDigest = true,
            },
            maxLeafKeys: 128,
            shardCount: 1,
            factory: grainFactory);

        leaf.Grain = new BPlusLeafGrain(
            context, state, grainFactory, optionsResolver,
            TestMutationObservers.NoObservers(),
            TestOriginClusterIdResolver.Default());
        leaf.State = state;
        leaf.Reporter = reporter;
        leaf.Snapshot = snapshotStub;
        return leaf;
    }

    private static Task DeactivateFinalAdvanceLeafAsync(FinalAdvanceLeaf leaf, CancellationToken deadline) =>
        ((IGrainBase)leaf.Grain).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            deadline);

    /// <summary>
    /// Queues a real checkpoint advance at the WAL head through the projection
    /// seam, leaving it PENDING under the coalescing options.
    /// </summary>
    private static async Task QueueFinalAdvanceAsync(FinalAdvanceLeaf leaf, GrowingWal wal)
    {
        var projection = AsProjection(leaf.Grain);
        var target = wal.Head;
        projection.Apply(BuildSet(
            $"k{target}", "v"u8.ToArray(), hlcPhysical: 10 + target, treeId: FinalAdvanceTreeId));

        var writesBefore = leaf.State.WriteCount;
        await projection.SetCheckpointOffsetAsync(target, default);

        Assert.That(leaf.State.WriteCount, Is.EqualTo(writesBefore),
            "precondition: the advance must still be PENDING, or no persist runs under test.");
    }

    /// <summary>
    /// ACCEPTANCE 1, change 1. The deadline expires right after the teardown
    /// persist has published its pin - inside the tail's cursor report - so the
    /// snapshot capture and the <c>frontier_pin</c> barrier both find a
    /// torn-down activation. The durable pin for the final advance must
    /// already stand, at <c>min(persisted, coverage)</c>.
    /// <para>
    /// Coverage is restamped to the pending checkpoint first (the #3224 path),
    /// so the pin the final persist can publish is 3 and the pin any earlier
    /// publisher could have left is 0: the assertion distinguishes the two.
    /// Pre-fix the tail's batched pin ran AFTER the cursor report and read
    /// state, so it faulted with the rest and the leaf went dormant with no
    /// publication at 3. Remove the tail's first step and it goes red the same
    /// way: <c>frontier_pin</c> is the only other publisher and it skips.
    /// </para>
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_publishes_the_final_advance_pin_when_the_trailing_barriers_cannot_run()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0);
        await ActivateAsync(leaf.Grain);
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero),
                "precondition: the replay applied real entries, so the pin publishers are live.");
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
                "precondition: the advance to 3 is still PENDING; persisted is the rehydrated 0.");
            Assert.That(leaf.Grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "precondition: the pending advance is 3.");
            Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L),
                "precondition: coverage is 3, so the final persist's coverage-gated pin is 3.");
        });

        leaf.Published.Clear();
        var reasons = new List<string>();
        using var deadline = new CancellationTokenSource();
        leaf.OnCursorReport = () => leaf.TearDown(deadline);

        using (ListenForBarrierFailures(reasons))
        {
            // Must not throw: the trailing barriers skip on the torn-down activation.
            await DeactivateFinalAdvanceLeafAsync(leaf, deadline.Token);
        }

        leaf.State.ThrowOnStateAccess = null;

        Assert.Multiple(() =>
        {
            Assert.That(deadline.IsCancellationRequested, Is.True,
                "control: the teardown never landed, so nothing below was tested under it.");
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the teardown persist committed the final advance.");
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Does.Contain(3L),
                "THE assertion: the awaited durable pin for the final advance, "
                + "min(persisted 3, coverage 3) == 3, must have landed before the deadline tore the "
                + "activation down. Pre-fix nothing published it.");
            Assert.That(leaf.Published.All(p => p.PublishedOffset <= p.PersistedCheckpoint), Is.True,
                "the #3476 invariant: no publication exceeds the persisted checkpoint.");
            Assert.That(leaf.Published.All(p => p.PublishedOffset <= p.Coverage), Is.True,
                "the #945 invariant: no publication exceeds durable snapshot coverage.");
            Assert.That(reasons, Does.Contain(LatticeMetrics.DeactivationBarrierSnapshotCapture.Value),
                "the snapshot_capture barrier found a torn-down activation; its skip must be counted.");
            Assert.That(reasons, Does.Contain(LatticeMetrics.DeactivationBarrierFrontierPin.Value),
                "the frontier_pin barrier found a torn-down activation; its skip must be counted.");
            Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DeactivationBarrierCheckpointFlush.Value),
                "the teardown persist itself succeeded and must not be charged a barrier failure.");
            Assert.That(leaf.Warnings.Count(w => w.Contains("already torn down")), Is.GreaterThanOrEqualTo(2),
                "each skip must be logged as a teardown skip.");
        });
    }

    /// <summary>
    /// ACCEPTANCE 2. With the persisted checkpoint (3) above durable snapshot
    /// coverage (0), the teardown persist's pin is the COVERAGE value, never the
    /// checkpoint: WAL GC must not trim entries a cold rebuild still needs
    /// (#945). The capture that follows raises coverage, and only then does the
    /// <c>frontier_pin</c> barrier publish 3.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_caps_the_final_advance_pin_at_durable_snapshot_coverage()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0);
        await ActivateAsync(leaf.Grain);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "precondition: the pending advance is 3.");
            Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(0L),
                "precondition: durable coverage is the rehydrated 0, below the advance.");
        });

        leaf.Published.Clear();
        leaf.Calls.Clear();

        await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);

        var batched = leaf.Batched.ToList();
        Assert.That(batched, Is.Not.Empty, "control: the deactivation published no durable pin at all.");

        Assert.Multiple(() =>
        {
            Assert.That(batched[0], Is.EqualTo(new FinalAdvancePin(FinalAdvancePinChannel.Batched, 0L, 3L, 0L)),
                "THE assertion: the teardown persist's pin, published first, is against persisted 3 "
                + "and coverage 0, and must be the coverage value 0 - never the checkpoint 3.");
            Assert.That(batched.All(p => p.PublishedOffset == Math.Min(p.PersistedCheckpoint, p.Coverage)), Is.True,
                "every durable pin is min(persisted checkpoint, durable snapshot coverage).");
            Assert.That(leaf.Calls, Does.Contain("capture"),
                "control: the deactivation capture ran and raised coverage.");
            Assert.That(batched[^1].PublishedOffset, Is.EqualTo(3L),
                "control: once the capture covered 3 the frontier_pin barrier published 3.");
        });
    }

    /// <summary>
    /// ACCEPTANCE 4. No per-persist pin writes: on the ordinary persist path
    /// only the first real frontier goes through the batched flush and every
    /// later persist rides the debounced mirror, exactly as before; a graceful
    /// deactivation adds two batched flushes (the teardown persist's and the
    /// <c>frontier_pin</c> barrier's) regardless of how many persists preceded
    /// it. The pin grain's own batching is untouched by this change.
    /// </summary>
    [Test]
    public async Task Checkpoint_persists_write_no_extra_durable_pins_outside_the_teardown_persist()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0);
        await ActivateAsync(leaf.Grain);
        leaf.Published.Clear();

        // Two ordinary persists.
        await AsProjection(leaf.Grain).FlushCheckpointAsync();
        wal.GrowTo(5);
        await QueueFinalAdvanceAsync(leaf, wal);
        await AsProjection(leaf.Grain).FlushCheckpointAsync();

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Batched.Count(), Is.EqualTo(1),
                "the ordinary path publishes through the batched flush only on the first real frontier.");
            Assert.That(leaf.Mirrored.Count(), Is.EqualTo(1),
                "every later ordinary persist rides the debounced mirror, not a durable write.");
        });

        wal.GrowTo(7);
        await QueueFinalAdvanceAsync(leaf, wal);
        leaf.Published.Clear();

        await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Batched.Count(), Is.EqualTo(2),
                "a graceful deactivation publishes exactly twice: the teardown persist's tail and the "
                + "frontier_pin barrier. More would be a write-through; fewer means the tail did not publish.");
            Assert.That(leaf.Mirrored, Is.Empty,
                "the teardown persist replaces the debounced mirror rather than adding to it.");
        });
    }

    /// <summary>
    /// ACCEPTANCE 5, change 3. The deadline expires while the upward digest hop
    /// is in flight - the recorded drain's slow step. The snapshot capture and
    /// both pin publishes must already have completed, in that order, before
    /// the digest hop starts.
    /// <para>
    /// The coalescing window is the production default, so a pending coalesced
    /// digest exists: pre-fix its <c>digest_publish</c> barrier led the hook,
    /// the teardown landed inside it, and every durability barrier after it
    /// faulted.
    /// </para>
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_completes_capture_and_pin_before_a_slow_digest_publish()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(
            wal.Coordinator, digestCoalescingWindowMs: LatticeOptions.DefaultDigestCoalescingWindowMs);
        await ActivateAsync(leaf.Grain);
        leaf.Published.Clear();
        leaf.Calls.Clear();

        var reasons = new List<string>();
        using var deadline = new CancellationTokenSource();
        leaf.OnParentDigest = () => leaf.TearDown(deadline);

        using (ListenForBarrierFailures(reasons))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, deadline.Token);
        }

        leaf.State.ThrowOnStateAccess = null;

        var capture = leaf.Calls.IndexOf("capture");
        var finalPin = leaf.Calls.LastIndexOf("pin:3");
        var digest = leaf.Calls.IndexOf("digest");

        Assert.Multiple(() =>
        {
            Assert.That(digest, Is.GreaterThanOrEqualTo(0),
                "control: the digest hop never ran, so the deadline never expired inside it.");
            Assert.That(capture, Is.GreaterThanOrEqualTo(0).And.LessThan(digest),
                "the snapshot capture must complete before the slow digest hop starts.");
            Assert.That(finalPin, Is.GreaterThan(capture).And.LessThan(digest),
                "the post-capture pin, min(persisted 3, coverage 3), must land before the digest hop.");
            Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DeactivationBarrierSnapshotCapture.Value),
                "the capture ran on a live activation and must not be counted as skipped or faulted.");
            Assert.That(reasons, Does.Not.Contain(LatticeMetrics.DeactivationBarrierFrontierPin.Value),
                "the pin ran on a live activation and must not be counted as skipped or faulted.");
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the teardown persist committed the final advance.");
        });
    }

    /// <summary>
    /// Change 3 must keep the <c>deactivation_flush</c> outcome. Before the
    /// reorder the leading <c>digest_publish</c> barrier drained a pending
    /// coalesced publish and recorded <c>deactivation_flush</c>; the deferred
    /// teardown publish now supersedes that barrier, so it must record the same
    /// outcome exactly once when a coalesced publish was pending at entry, and
    /// hit the parent once, not twice.
    /// <para>
    /// The zero-window case is the control that keeps the assertion honest:
    /// coalescing is off, nothing was pending, and the same publish is the
    /// ordinary <c>inline</c> one. A fixture that recorded
    /// <c>deactivation_flush</c> on every deferred publish, or on none, fails
    /// one of the two cases.
    /// </para>
    /// </summary>
    [TestCase(LatticeOptions.DefaultDigestCoalescingWindowMs, 1, 0)]
    [TestCase(0, 0, 1)]
    public async Task OnDeactivateAsync_deferred_digest_records_deactivation_flush_only_when_a_coalesced_publish_was_pending(
        int windowMs, int expectedDeactivationFlush, int expectedInline)
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: windowMs);
        await ActivateAsync(leaf.Grain);
        leaf.Calls.Clear();

        var paths = new List<string>();
        using (MeterListening.StartForInstrument(
            LatticeMetrics.LeafDigestPublishes,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? tree = null;
                string? path = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value as string;
                    else if (tag.Key == LatticeMetrics.TagPath) path = tag.Value as string;
                }

                if (tree != FinalAdvanceTreeId || path is null) return;
                lock (paths)
                {
                    for (var i = 0; i < value; i++) paths.Add(path);
                }
            })))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, CancellationToken.None);
        }

        List<string> recorded;
        lock (paths) recorded = [.. paths];

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the teardown persist ran, so the deferred digest publish had work to do.");
            Assert.That(recorded.Count(p => p == (string)LatticeMetrics.PathDeactivationFlushTag.Value!),
                Is.EqualTo(expectedDeactivationFlush),
                "deactivation_flush must be recorded exactly when a coalesced publish was pending at entry.");
            Assert.That(recorded.Count(p => p == (string)LatticeMetrics.PathInlineTag.Value!),
                Is.EqualTo(expectedInline),
                "the deferred publish is recorded under one outcome, never both.");
            Assert.That(leaf.Calls.Count(c => c == "digest"), Is.EqualTo(1),
                "the parent must receive the teardown digest exactly once.");
        });
    }

    /// <summary>
    /// A graceful deactivation whose deadline has already expired when the
    /// hook starts must not throw, and counts a skip for each trailing barrier.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_on_an_expired_deadline_counts_a_skip_for_each_trailing_barrier()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0);
        await ActivateAsync(leaf.Grain);
        leaf.Published.Clear();

        var reasons = new List<string>();
        using var deadline = new CancellationTokenSource();
        deadline.Cancel();

        using (ListenForBarrierFailures(reasons))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, deadline.Token);
        }

        Assert.Multiple(() =>
        {
            Assert.That(reasons.Count(r => r == (string)LatticeMetrics.DeactivationBarrierSnapshotCapture.Value!), Is.EqualTo(1),
                "the snapshot_capture skip must be counted once under its existing reason.");
            Assert.That(reasons.Count(r => r == (string)LatticeMetrics.DeactivationBarrierFrontierPin.Value!), Is.EqualTo(1),
                "the frontier_pin skip must be counted once under its existing reason.");
            Assert.That(leaf.Batched, Is.Empty,
                "a skipped barrier must not publish.");
        });
    }

    /// <summary>The two trailing barriers under test, by their existing reason value.</summary>
    private static readonly string[] TrailingBarriers =
    [
        (string)LatticeMetrics.DeactivationBarrierSnapshotCapture.Value!,
        (string)LatticeMetrics.DeactivationBarrierFrontierPin.Value!,
    ];

    private static Task InvokeTrailingBarrierAsync(FinalAdvanceLeaf leaf, string barrier, CancellationToken ct) =>
        string.Equals(barrier, (string)LatticeMetrics.DeactivationBarrierSnapshotCapture.Value!, StringComparison.Ordinal)
            ? leaf.Grain.TryCaptureSnapshotOnDeactivateAsync(ct)
            : leaf.Grain.FlushDurableMaterialiserFrontierOnDeactivateAsync(ct);

    private static async Task<FinalAdvanceLeaf> CreateActivatedLeafWithPersistedAdvanceAsync()
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0);
        await ActivateAsync(leaf.Grain);

        // Persist the advance so the capture's #1535 gate admits a capture and
        // the pin flush has a real frontier: both barriers then have work to
        // do, and a skip is distinguishable from a no-op.
        await AsProjection(leaf.Grain).FlushCheckpointAsync();
        leaf.Published.Clear();
        leaf.Calls.Clear();
        leaf.Warnings.Clear();
        return leaf;
    }

    /// <summary>
    /// ACCEPTANCE 3. A barrier whose token is already cancelled skips BEFORE its
    /// first state read, counts the skip under its own reason, logs it, and
    /// does no work.
    /// </summary>
    [TestCaseSource(nameof(TrailingBarriers))]
    public async Task Trailing_barrier_on_a_cancelled_token_skips_before_reading_state(string barrier)
    {
        var leaf = await CreateActivatedLeafWithPersistedAdvanceAsync();

        var stateReads = 0;
        leaf.State.OnStateAccess = () => stateReads++;
        using var deadline = new CancellationTokenSource();
        deadline.Cancel();

        var reasons = new List<string>();
        using (ListenForBarrierFailures(reasons))
        {
            await InvokeTrailingBarrierAsync(leaf, barrier, deadline.Token);
        }

        leaf.State.OnStateAccess = null;

        Assert.Multiple(() =>
        {
            Assert.That(reasons, Is.EqualTo(new[] { barrier }),
                "exactly one skip, under the barrier's existing reason value.");
            Assert.That(leaf.Calls, Is.Empty,
                "a skipped barrier must neither capture nor publish.");
            Assert.That(leaf.Warnings.Count(w => w.Contains("already torn down")), Is.EqualTo(1),
                "the skip must be logged as a teardown skip.");
        });

        // The skip itself resolves its tags defensively; the barrier's WORK
        // must not have read state. Tag resolution reads it at most twice.
        Assert.That(stateReads, Is.LessThanOrEqualTo(2),
            "the token must be checked before the barrier's first state read.");
    }

    /// <summary>
    /// ACCEPTANCE 3. The deadline lands mid-barrier: the state read throws the
    /// recorded invalid-activation fault while the token is cancelled. The
    /// barrier catches it, counts a skip, and does not throw.
    /// </summary>
    [TestCaseSource(nameof(TrailingBarriers))]
    public async Task Trailing_barrier_skips_an_invalid_activation_fault_raised_after_the_deadline(string barrier)
    {
        var leaf = await CreateActivatedLeafWithPersistedAdvanceAsync();

        using var deadline = new CancellationTokenSource();
        leaf.State.OnStateAccess = () =>
        {
            deadline.Cancel();
            throw new InvalidOperationException(InvalidActivationMessage);
        };

        var reasons = new List<string>();
        using (ListenForBarrierFailures(reasons))
        {
            await InvokeTrailingBarrierAsync(leaf, barrier, deadline.Token);
        }

        leaf.State.OnStateAccess = null;

        Assert.Multiple(() =>
        {
            Assert.That(reasons, Is.EqualTo(new[] { barrier }),
                "the torn-down activation's fault must be counted as one skip under the barrier's reason.");
            Assert.That(leaf.Warnings.Count(w => w.Contains("already torn down")), Is.EqualTo(1),
                "and logged as a teardown skip.");
        });
    }

    /// <summary>
    /// ACCEPTANCE 3, the narrowness half. The SAME exception with the token NOT
    /// cancelled is a genuine defect and must still propagate, so the barrier's
    /// own fault containment reports it as a fault. Without this the catch
    /// could silently widen to every <see cref="InvalidOperationException"/>.
    /// </summary>
    [TestCaseSource(nameof(TrailingBarriers))]
    public async Task Trailing_barrier_propagates_an_invalid_activation_fault_while_the_deadline_is_live(string barrier)
    {
        var leaf = await CreateActivatedLeafWithPersistedAdvanceAsync();

        using var deadline = new CancellationTokenSource();
        leaf.State.OnStateAccess = () => throw new InvalidOperationException(InvalidActivationMessage);

        var reasons = new List<string>();
        using (ListenForBarrierFailures(reasons))
        {
            var thrown = Assert.ThrowsAsync<InvalidOperationException>(
                () => InvokeTrailingBarrierAsync(leaf, barrier, deadline.Token));
            Assert.That(thrown!.Message, Is.EqualTo(InvalidActivationMessage));
        }

        leaf.State.OnStateAccess = null;

        Assert.That(reasons, Is.Empty,
            "a propagated fault is not a skip; the barrier's containment counts it, not the skip path.");
    }

    /// <summary>
    /// ACCEPTANCE 3, the domain-fault exclusion. An
    /// <see cref="ILatticeDomainFault"/> is a real refusal, not a torn-down
    /// activation, and must propagate even when the deadline has expired -
    /// <see cref="LatticeShuttingDownException"/> is an
    /// <see cref="InvalidOperationException"/>, so only the exclusion stops the
    /// catch from swallowing it.
    /// </summary>
    [TestCaseSource(nameof(TrailingBarriers))]
    public async Task Trailing_barrier_propagates_a_domain_fault_even_after_the_deadline(string barrier)
    {
        var leaf = await CreateActivatedLeafWithPersistedAdvanceAsync();

        using var deadline = new CancellationTokenSource();
        leaf.State.OnStateAccess = () =>
        {
            deadline.Cancel();
            throw new LatticeShuttingDownException("domain refusal");
        };

        var reasons = new List<string>();
        using (ListenForBarrierFailures(reasons))
        {
            Assert.ThrowsAsync<LatticeShuttingDownException>(
                () => InvokeTrailingBarrierAsync(leaf, barrier, deadline.Token));
        }

        leaf.State.OnStateAccess = null;

        Assert.That(reasons, Is.Empty, "a domain fault must never be recorded as a teardown skip.");
    }
}
