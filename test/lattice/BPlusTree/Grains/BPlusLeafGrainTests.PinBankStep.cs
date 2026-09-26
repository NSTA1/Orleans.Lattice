using NSubstitute;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3599: the permit-free durable pin bank step, and the persist-tail
/// ordering that must publish the pin AFTER the tail's own snapshot capture.
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect.</b> The teardown persist's tail published the durable pin
/// before its snapshot recheck, so it published pre-capture coverage, and
/// nothing republished the pin afterwards except the <c>frontier_pin</c>
/// barrier, which a deactivation deadline can skip, and the starvation drive,
/// which takes a replay permit. A write-idle leaf then held the shared WAL's
/// trim floor at a stale pin forever.
/// </para>
/// <para>
/// <b>The safety invariant these fixtures also hold.</b> The published pin is
/// <c>min(persisted checkpoint, durable coverage)</c> with coverage read after
/// the capture's outcome. A capture that is declined or throws must leave the
/// pin clamped at the EXISTING coverage: the WAL between coverage and the
/// checkpoint is the only durable copy of those rows and deletes.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Reads the process-wide replay gate, which the harness activation has
    /// already sized.
    /// </summary>
    private static SemaphoreSlim ActivatedReplayGate()
    {
        var gate = BPlusLeafGrain.ReplayConcurrencyGateForTest;
        Assert.That(gate, Is.Not.Null,
            "precondition: a completed activation with a tree id sizes the process-wide replay gate.");
        return gate!;
    }

    /// <summary>
    /// Activates a harness leaf over a WAL of three entries: the replayed
    /// advance to 3 is PENDING, the persisted checkpoint is 0, coverage is 0.
    /// </summary>
    private static async Task<(FinalAdvanceLeaf Leaf, GrowingWal Wal)> ActivatePinBankLeafAsync(
        int reclassifyEveryNCheckpoints = 1000)
    {
        var wal = new GrowingWal();
        wal.GrowTo(3);
        var leaf = CreateFinalAdvanceLeaf(
            wal.Coordinator, digestCoalescingWindowMs: 0, reclassifyEveryNCheckpoints);
        await ActivateAsync(leaf.Grain);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
                "precondition: the advance to 3 is still PENDING; persisted is the rehydrated 0.");
            Assert.That(leaf.Grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "precondition: the pending advance is 3.");
            Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(0L),
                "precondition: coverage is the rehydrated 0.");
        });

        return (leaf, wal);
    }

    /// <summary>
    /// Test (i). The teardown persist's tail captures on its cadence, and the
    /// deadline expires inside the tail's cursor report, so the trailing
    /// <c>frontier_pin</c> barrier skips. The durable pin must already stand at
    /// <c>min(persisted 3, POST-capture coverage 3)</c>. Pre-fix the tail
    /// published before its recheck and so published <c>min(3, 0) = 0</c>, and
    /// the only later publisher skipped.
    /// </summary>
    [Test]
    public async Task OnDeactivateAsync_teardown_tail_publishes_the_post_capture_pin_when_frontier_pin_is_skipped()
    {
        var (leaf, _) = await ActivatePinBankLeafAsync(reclassifyEveryNCheckpoints: 1);

        leaf.Published.Clear();
        leaf.Calls.Clear();
        var reasons = new List<string>();
        using var deadline = new CancellationTokenSource();
        leaf.OnCursorReport = () => leaf.TearDown(deadline);

        using (ListenForBarrierFailures(reasons))
        {
            await DeactivateFinalAdvanceLeafAsync(leaf, deadline.Token);
        }

        leaf.State.ThrowOnStateAccess = null;

        var captureAt = leaf.Calls.IndexOf("capture");
        var pinAt = leaf.Calls.IndexOf("pin:3");
        Assert.Multiple(() =>
        {
            Assert.That(deadline.IsCancellationRequested, Is.True,
                "control: the teardown never landed, so frontier_pin was not skipped under test.");
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the teardown persist committed the final advance.");
            Assert.That(captureAt, Is.GreaterThanOrEqualTo(0),
                "control: the tail's cadence recheck captured, raising coverage to 3.");
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Does.Contain(3L),
                "THE assertion: the tail's awaited pin is min(persisted 3, post-capture coverage 3). A "
                    + "tail that publishes before its capture publishes 0 here, and with frontier_pin "
                    + "skipped nothing republishes it.");
            Assert.That(pinAt, Is.GreaterThan(captureAt),
                "the publish must follow the capture it reads coverage from.");
            Assert.That(leaf.Batched.All(p => p.PublishedOffset <= Math.Min(p.PersistedCheckpoint, p.Coverage)),
                Is.True,
                "safety: every published pin is clamped by the coverage standing when it was published.");
        });
    }

    /// <summary>
    /// Test (ii). A write-idle leaf whose persisted checkpoint and coverage
    /// both reached 3 while its durable pin still stood at 0. The next
    /// coverage-lag tick must publish 3, and must do it without a replay
    /// permit and without reading the WAL.
    /// </summary>
    [Test]
    public async Task OnCoverageLagTimerTickAsync_raises_a_lagging_pin_on_a_write_idle_leaf_without_a_replay_permit()
    {
        var (leaf, wal) = await ActivatePinBankLeafAsync();

        // Capture to 3 on a tick (persisted is still 0, so this tick banks 0),
        // then persist the checkpoint. The ordinary persist tail captures
        // nothing new on its cadence, so it publishes nothing: the pin is now
        // below min(persisted 3, coverage 3).
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        await AsProjection(leaf.Grain).FlushCheckpointAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "precondition: the checkpoint is persisted at 3.");
            Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L),
                "precondition: coverage is 3.");
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Does.Not.Contain(3L),
                "precondition: nothing has yet published 3, so the tick under test is the publisher.");
        });

        var gate = ActivatedReplayGate();
        var baseline = gate.CurrentCount;
        var observedDuringPublish = -1;
        var walReads = 0;
        leaf.OnPinFlush = () => observedDuringPublish = gate.CurrentCount;
        wal.OnRead = () =>
        {
            walReads++;
            return Task.CompletedTask;
        };
        leaf.Published.Clear();

        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Does.Contain(3L),
                "the tick must bank the pin at min(persisted 3, coverage 3) even though no capture was "
                    + "needed - the pin, not the coverage, is what lagged.");
            Assert.That(observedDuringPublish, Is.EqualTo(baseline),
                "the tick must hold no replay permit while it publishes.");
            Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                "and must hold none afterwards.");
            Assert.That(walReads, Is.Zero,
                "the bank step replays nothing: it works only on state the activation already holds.");
        });

        // Idempotent: a second tick with nothing new to bank publishes nothing.
        leaf.Published.Clear();
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        Assert.That(leaf.Batched, Is.Empty,
            "a tick whose pin already stands at min(persisted, coverage) must not republish it.");
    }

    /// <summary>
    /// Test (iii), capture throws. The bank step commits the pending advance
    /// (persisted 3) but its capture fails, so coverage stays 0. The published
    /// pin must be clamped at <c>min(3, existing coverage 0)</c> and never 3.
    /// </summary>
    [Test]
    public async Task BankDurablePinAsync_when_the_capture_throws_publishes_no_higher_than_the_existing_coverage()
    {
        var (leaf, _) = await ActivatePinBankLeafAsync();
        var attempts = 0;
        leaf.Snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns<Task<LeafSnapshotSaveOutcome>>(_ =>
            {
                attempts++;
                throw new InvalidOperationException("snapshot store unavailable");
            });
        leaf.Published.Clear();

        await leaf.Grain.BankDurablePinAsync();

        AssertPinClampedAtExistingCoverage(leaf, attempts);
    }

    /// <summary>
    /// Test (v), capture declined. A save the store declines restamps no
    /// coverage, so the pin must stay clamped at the OLD coverage.
    /// </summary>
    [Test]
    public async Task BankDurablePinAsync_when_the_capture_is_declined_keeps_the_pin_at_the_old_coverage()
    {
        var (leaf, _) = await ActivatePinBankLeafAsync();
        var attempts = 0;
        leaf.Snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                attempts++;
                return Task.FromResult(LeafSnapshotSaveOutcome.Declined);
            });
        leaf.Published.Clear();

        await leaf.Grain.BankDurablePinAsync();

        AssertPinClampedAtExistingCoverage(leaf, attempts);
    }

    private static void AssertPinClampedAtExistingCoverage(FinalAdvanceLeaf leaf, int captureAttempts)
    {
        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "control: the bank step committed the pending advance, so persisted is 3 and only the "
                    + "coverage clamp stands between the pin and 3.");
            Assert.That(captureAttempts, Is.GreaterThan(0),
                "control: the capture was attempted, or its failure was not tested.");
            Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(0L),
                "control: a capture the store did not keep leaves coverage where it was.");
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Is.All.LessThanOrEqualTo(0L),
                "SAFETY: the pin may never exceed min(persisted 3, existing coverage 0). The WAL above 0 is "
                    + "the only durable copy of those rows and deletes; a pin at 3 would license trimming it "
                    + "and a rehydrate from the coverage-0 snapshot would resurrect the deletes.");
            Assert.That(leaf.Mirrored.Select(p => p.PublishedOffset), Is.All.LessThanOrEqualTo(0L),
                "and the debounced mirror is held to the same clamp.");
        });
    }

    /// <summary>
    /// The bank step on the grain surface commits a pending advance, captures,
    /// and publishes the new pin in one permit-free call - the shape the WAL
    /// GC's first tier relies on.
    /// </summary>
    [Test]
    public async Task BankDurablePinAsync_commits_captures_and_publishes_without_a_replay_permit()
    {
        var (leaf, wal) = await ActivatePinBankLeafAsync();
        var gate = ActivatedReplayGate();
        var baseline = gate.CurrentCount;
        var observedDuringCapture = -1;
        var walReads = 0;
        leaf.Snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                observedDuringCapture = gate.CurrentCount;
                leaf.Calls.Add("capture");
                return Task.FromResult(LeafSnapshotSaveOutcome.Kept);
            });
        wal.OnRead = () =>
        {
            walReads++;
            return Task.CompletedTask;
        };
        leaf.Published.Clear();

        await leaf.Grain.BankDurablePinAsync();

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "the pending advance must be committed.");
            Assert.That(leaf.Grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L),
                "the capture must land coverage at the committed checkpoint.");
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Does.Contain(3L),
                "the pin must be published at min(persisted 3, post-capture coverage 3).");
            Assert.That(observedDuringCapture, Is.EqualTo(baseline),
                "the capture must run without a replay permit.");
            Assert.That(gate.CurrentCount, Is.EqualTo(baseline),
                "and no permit may be held afterwards.");
            Assert.That(walReads, Is.Zero, "the bank step must read nothing from the WAL.");
        });
    }

    /// <summary>
    /// A leaf with no tree id bound has nothing to bank: the call must return
    /// without touching the reporter or the snapshot store.
    /// </summary>
    [Test]
    public async Task BankDurablePinAsync_on_an_unbound_leaf_does_nothing()
    {
        var wal = new GrowingWal();
        var leaf = CreateFinalAdvanceLeaf(wal.Coordinator, digestCoalescingWindowMs: 0);
        leaf.State.State.TreeId = null!;

        await leaf.Grain.BankDurablePinAsync();

        Assert.Multiple(() =>
        {
            Assert.That(leaf.Published, Is.Empty, "an unbound leaf publishes no pin.");
            Assert.That(leaf.Calls, Is.Empty, "and captures nothing.");
        });
    }
}
