using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Progress guarantees for a replay whose window opens with a deferred
/// terminal, and the guard on the invariant that bounds them.
/// <para>
/// The incremental replay flush (#1513) is CALLED after every non-empty slice
/// but BANKS conditionally. <c>TryFlushRecoveredCeilingAsync</c> clamps the
/// ceiling below the lowest unresolved deferred terminal and then returns
/// without persisting when the clamped ceiling has not passed the current
/// checkpoint. When a partition's window OPENS with a deferred
/// <see cref="MutationKind.DeleteRange"/> and the durable unresolved-work
/// ledger is saturated, that clamp evaluates to <c>minDeferred - 1 == 0</c> on
/// every slice, so an activation that is torn down part-way banks NOTHING and
/// the next one replays the identical window. That defect is tracked as
/// <c>#2746</c> and its reproduction ships here <c>[Ignore]</c>d; the three
/// arms that DO run are the controls that isolate it and the tripwire that
/// bounds it.
/// <para>
/// For a PREPARE the clamp is retired - #2165's
/// <c>EnsureUnresolvedPrepareRecorded</c> records unconditionally and
/// <c>MinUnresolvedPrepareOffsetForPartition</c> skips anything recorded. For
/// a deferred TERMINAL it is retained: the deferral site calls the CAPPED
/// <c>TryRecordUnresolvedReplayWork</c> and falls back to the in-memory clamp
/// once <c>MaxDurableUnresolvedReplayWork</c> is reached.
/// </para>
/// <para>
/// The nearest existing fixtures each miss this by one axis.
/// <c>Cross_partition_delete_range_still_holds_the_incremental_ceiling</c>
/// saturates the ledger across two partitions but puts the range delete at
/// offset 3, so the prefix below it banks and the replay runs to completion.
/// <c>Interrupted_replay_advances_checkpoint_when_window_opens_with_a_deferred_mutation</c>
/// does open the window at offset 1 and does tear the replay down, but on a
/// SINGLE partition - which makes that partition the one absorbed last, so the
/// range delete drains inline (#1831) and never reaches the clamp at all.
/// Reproducing it needs both at once.
/// </para>
/// <para>
/// REACHABILITY - this is a bounded residue rather than a live defect, and the
/// bound is load-bearing enough to carry its own guard. The obvious
/// escalation, that a repeatedly-failing leaf accumulates unresolved deferred
/// terminals across activations until the shipping cap of 1024 is reached,
/// DOES NOT HAPPEN: <c>RestoreUnresolvedReplayWork</c> drops every entry above
/// its partition's persisted checkpoint, because the replay is about to
/// re-read it and the WAL must stay its single source. On a leaf whose
/// checkpoint is still 0 that prunes the ledger to empty on every activation.
/// The cap is therefore only reachable WITHIN one activation, by 1024 deferred
/// terminals on partitions swept before the one whose window opens with a
/// range delete - which has not been shown to happen on any tree here.
/// <see cref="A_red_here_means_the_durable_ledger_now_accumulates_and_this_defect_is_live"/>
/// is the guard on exactly that, and its name says what a red means.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Partition 0 opens its window with a deferred range delete and is
    /// deliberately given the SMALLER backlog, because pass 1 sweeps by
    /// backlog ascending (#2089) and only the partition absorbed LAST is
    /// drain-eligible. That makes partition 0 genuinely non-last, so its range
    /// delete defers rather than draining inline.
    /// </summary>
    private static ILeafReplayCoordinatorGrain[] BuildDeferredClampCoordinators(
        Action<int>? onPartitionZeroRead,
        long deferredAt)
    {
        var p0 = new CommitLogSliceEntry[12];
        for (var i = 1; i <= 12; i++)
            p0[i - 1] = i == deferredAt ? FlushDeleteRange(i) : FlushSet(i, $"z{i:D2}");

        var p1 = new CommitLogSliceEntry[20];
        for (var i = 1; i <= 20; i++)
            p1[i - 1] = FlushSet(i, $"y{i:D2}");

        return
        [
            BuildObservableCoordinator(head: 12, sliceSize: 4, tail: 0, onRead: onPartitionZeroRead, p0),
            BuildObservableCoordinator(head: 20, sliceSize: 4, tail: 0, onRead: null, p1),
        ];
    }

    /// <summary>
    /// Runs one activation that is torn down after
    /// <paramref name="succeedReads"/> slices of partition 0, and returns the
    /// durable checkpoint the activation left behind.
    /// </summary>
    private static async Task<(long Checkpoint, List<long> Persists, Exception? Fault)> RunInterruptedDeferredReplayAsync(
        int succeedReads,
        long deferredAt,
        int maxDurableUnresolvedReplayWork,
        int preSeededLedgerEntries = 0)
    {
        var state = NewFlushCeilingState();

        // Reproduces a durable ledger carried in from earlier activations.
        // The seeded offsets sit far ABOVE this replay's window and on the
        // other partition, so they consume ledger capacity without clamping
        // any ceiling themselves - the cap is global, not per-partition.
        if (preSeededLedgerEntries > 0)
        {
            var ledger = state.State.UnresolvedReplayWork ??= [];
            for (var i = 0; i < preSeededLedgerEntries; i++)
            {
                ledger.Add(new UnresolvedReplayWorkEntry(
                    1,
                    100_000L + i,
                    BuildDeleteRange("m0", "m9", hlcPhysical: 500, treeId: FlushCeilingTreeId)));
            }
        }

        var persists = new List<long>();
        state.OnWriteState = s => persists.Add(s.ProjectionCheckpointOffset);

        using var cts = new CancellationTokenSource();
        var coordinators = BuildDeferredClampCoordinators(
            onPartitionZeroRead: read =>
            {
                if (read >= succeedReads)
                    cts.Cancel();
            },
            deferredAt);

        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(
            state,
            coordinators,
            store.Stub,
            maxDurableUnresolvedReplayWork: maxDurableUnresolvedReplayWork);

        Exception? fault = null;
        try
        {
            await ((IGrainBase)grain).OnActivateAsync(cts.Token);
        }
        catch (Exception ex)
        {
            fault = ex;
        }

        return (state.State.ProjectionCheckpointOffset, persists, fault);
    }

    [Test]
    [Ignore("Reproduces the open defect in issue #2746: the deferred-terminal clamp banks nothing "
        + "when a replay window opens with a deferred range delete and the ledger cannot absorb the "
        + "offset. Un-ignore this to verify the fix - a GREEN here means an interrupted replay now "
        + "banks its scanned prefix instead of repeating the identical window forever, and #2746 can "
        + "be closed. Do not delete or weaken this arm to make the suite green.")]
    public async Task An_interrupted_replay_banks_progress_even_when_a_deferred_range_delete_opens_the_window()
    {
        // THE ARM UNDER TEST. Partition 0's window opens with a deferred
        // DeleteRange at offset 1 and the ledger is saturated, so the offset
        // falls back to the in-memory clamp: every foot-of-loop flush computes
        // a ceiling of minDeferred - 1 == 0, which has not passed the
        // persisted checkpoint of 0, so nothing is persisted. The slice is
        // still SCANNED. The activation is then torn down having banked
        // nothing, and the next one faces the identical window - the #1513
        // livelock, reached through the one clamp arm #2165 did not retire.
        var (checkpoint, persists, fault) = await RunInterruptedDeferredReplayAsync(
            succeedReads: 2,
            deferredAt: 1,
            maxDurableUnresolvedReplayWork: 0);

        Assert.That(fault, Is.InstanceOf<OperationCanceledException>(),
            "The teardown must actually have ended the activation, or this arm proves nothing.");

        Assert.That(checkpoint, Is.GreaterThan(0L),
            "An interrupted replay made ZERO durable progress, because the deferred range delete at the "
            + "head of the window clamped every incremental flush back to the persisted checkpoint. The "
            + "next activation replays the identical window and is interrupted identically. Persisted "
            + $"checkpoints seen: [{string.Join(", ", persists)}].");
    }

    [Test]
    public async Task The_same_replay_banks_normally_when_the_deferred_offset_is_ledgered()
    {
        // CONTROL - isolates ledger saturation as a necessary condition.
        var (checkpoint, _, fault) = await RunInterruptedDeferredReplayAsync(
            succeedReads: 2,
            deferredAt: 1,
            maxDurableUnresolvedReplayWork: LatticeOptions.DefaultMaxDurableUnresolvedReplayWork);

        Assert.That(fault, Is.InstanceOf<OperationCanceledException>());
        Assert.That(checkpoint, Is.GreaterThan(0L),
            "With the deferred offset ledgered the ceiling is unclamped, so the scanned prefix must bank.");
    }

    [Test]
    public async Task The_same_replay_banks_normally_when_the_deferred_offset_is_not_at_the_head()
    {
        // CONTROL - isolates the head-of-window position as the other
        // necessary condition, so the failing arm is not merely "a saturated
        // ledger breaks banking".
        var (checkpoint, _, fault) = await RunInterruptedDeferredReplayAsync(
            succeedReads: 2,
            deferredAt: 5,
            maxDurableUnresolvedReplayWork: 0);

        Assert.That(fault, Is.InstanceOf<OperationCanceledException>());
        Assert.That(checkpoint, Is.GreaterThan(0L),
            "A deferred terminal below the scanned prefix must not stop that prefix banking.");
    }

    [Test]
    public async Task A_red_here_means_the_durable_ledger_now_accumulates_and_this_defect_is_live()
    {
        // ESCALATION TRIPWIRE - a red here is NOT "a test broke". It means the
        // pruning invariant that bounds the clamp above has been weakened, the
        // cross-activation accumulation route has opened, and the head-of-
        // window clamp is reachable at the SHIPPING cap rather than only at an
        // artificial cap of 0. Treat it as a live correctness defect in WAL
        // replay and escalate rather than adjusting this test.
        //
        // The invariant it guards is in RestoreUnresolvedReplayWork:
        //
        //     if (entry.Offset > checkpoint)
        //         continue; // Replay re-reads it; the WAL stays the single source.
        //
        // Every entry above its partition's persisted checkpoint is dropped,
        // so a leaf whose checkpoint is still 0 prunes its ledger to empty on
        // every activation and can never carry a full ledger in. This arm
        // seeds a FULL ledger at the shipping cap and asserts that it still
        // does not starve the flush.
        var (checkpoint, _, fault) = await RunInterruptedDeferredReplayAsync(
            succeedReads: 2,
            deferredAt: 1,
            maxDurableUnresolvedReplayWork: LatticeOptions.DefaultMaxDurableUnresolvedReplayWork,
            preSeededLedgerEntries: LatticeOptions.DefaultMaxDurableUnresolvedReplayWork);

        Assert.That(fault, Is.InstanceOf<OperationCanceledException>());
        Assert.That(checkpoint, Is.GreaterThan(0L),
            "ESCALATE: a durable ledger carried in full from earlier activations has starved the "
            + "incremental flush. RestoreUnresolvedReplayWork is no longer pruning entries above the "
            + "persisted checkpoint, so the deferred-terminal clamp is now reachable at the shipping "
            + "MaxDurableUnresolvedReplayWork cap and an interrupted replay can make zero durable "
            + "progress in production. This is a live WAL-replay correctness defect, not a broken test.");
    }
}
