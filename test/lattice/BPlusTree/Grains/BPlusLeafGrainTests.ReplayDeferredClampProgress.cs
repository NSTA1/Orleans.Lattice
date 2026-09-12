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
/// REACHABILITY. <c>RestoreUnresolvedReplayWork</c> drops every entry ABOVE
/// its partition's persisted checkpoint, because the replay is about to
/// re-read it and the WAL must stay its single source; entries at or below the
/// checkpoint are KEPT. The invariant is exactly "the ledger covers the
/// offsets this replay will not re-read".
/// </para>
/// <para>
/// An earlier revision of this file read that prune as a general bound and
/// said the cap was "only reachable WITHIN one activation". That was wrong,
/// and it is corrected here rather than quietly deleted because it is the
/// inference a reader is most likely to make again. It holds only for a leaf
/// pinned at checkpoint 0, where EVERY entry is above the checkpoint and the
/// ledger does prune to empty on every activation. Above checkpoint 0 the
/// ledger genuinely carries in, and the two kinds it carries are asymmetric: a
/// kept deferred TERMINAL is handed to pass 2 and drains, but a kept
/// unresolved PREPARE is re-applied and nothing reaps it (#2304).
/// </para>
/// <para>
/// So a STOCK route to the cap exists, with no configuration deviation.
/// Prepares accumulate through <c>EnsureUnresolvedPrepareRecorded</c>, which
/// is UNCAPPED by design (#2183) yet shares one list with the capped terminal
/// writer, so they consume the budget that bounds terminals. Once the shared
/// list reaches the deployed cap, an ordinary deferred terminal at a window
/// head is refused and this clamp fires at 1024. That route needs a tree that
/// runs sagas, and the candidate nominated for it -
/// <c>repo-context-vector-index</c> - is written NON-ATOMICALLY and carries no
/// prepares at all, which is why the deferred arm is the only clamp that can
/// fire there. The stock accumulation route and that candidate are therefore
/// mutually exclusive: both are real, neither is reachable on the same tree.
/// </para>
/// <para>
/// OBSERVABILITY. Whether this fires in production is currently unanswerable
/// in either direction. The refusal at
/// <c>BPlusLeafGrain.Activation.cs</c> emits no metric, no log and no counter
/// - it simply falls back to <c>deferredOffsets.Add</c>. The one adjacent
/// instrument, <c>LeafUnresolvedPrepareLedgerBeyondCap</c>, belongs to the
/// prepare path and is off by one in the blind direction: the terminal writer
/// refuses at <c>work.Count &gt;= cap</c>, so the FIRST refusal happens at a
/// count of exactly <c>cap</c>, while the instrument fires only at
/// <c>work.Count &gt; cap</c>. A ledger resting exactly at the cap drops every
/// terminal offered to it and never increments anything.
/// </para>
/// <para>
/// <see cref="A_red_here_means_the_durable_ledger_now_accumulates_and_this_defect_is_live"/>
/// guards the prune DIRECTION only, which is the part that is load-bearing
/// here. It is not a guard against accumulation as such, because accumulation
/// below the checkpoint is legitimate and expected.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// Three partitions, because two cannot reach the defect at a positive
    /// cap. Pass 1 sweeps by backlog ASCENDING (#2089) and only the partition
    /// absorbed LAST is drain-eligible, so with two partitions the one whose
    /// window opens with a range delete is either swept FIRST, where the
    /// ledger is still empty and its offset records fine, or swept LAST, where
    /// the range delete drains inline (#1831). Neither reaches the refusal.
    /// Only a MIDDLE partition is neither, so a third is required.
    /// <para>
    /// Backlogs are therefore chosen for sweep POSITION, not for size:
    /// partition 1 (8) is absorbed first and its own deferred range delete is
    /// what fills the ledger; partition 0 (12) is the middle one under test;
    /// partition 2 (20) is absorbed last and is the drain-eligible one.
    /// </para>
    /// </summary>
    private static ILeafReplayCoordinatorGrain[] BuildDeferredClampCoordinators(
        Action<int>? onPartitionZeroRead,
        long deferredAt)
    {
        var p0 = new CommitLogSliceEntry[12];
        for (var i = 1; i <= 12; i++)
            p0[i - 1] = i == deferredAt ? FlushDeleteRange(i) : FlushSet(i, $"z{i:D2}");

        // Swept FIRST. Its range delete defers (this partition is not the last
        // absorbed) and so is recorded durably, occupying the ledger before
        // partition 0 is swept at all. This is what saturates a positive cap
        // WITHOUT any pre-seeding, which matters because a pre-seeded entry
        // cannot survive into the window on a checkpoint-0 leaf.
        var p1 = new CommitLogSliceEntry[8];
        for (var i = 1; i <= 8; i++)
            p1[i - 1] = i == 4 ? FlushDeleteRange(i) : FlushSet(i, $"y{i:D2}");

        var p2 = new CommitLogSliceEntry[20];
        for (var i = 1; i <= 20; i++)
            p2[i - 1] = FlushSet(i, $"x{i:D2}");

        return
        [
            BuildObservableCoordinator(head: 12, sliceSize: 4, tail: 0, onRead: onPartitionZeroRead, p0),
            BuildObservableCoordinator(head: 8, sliceSize: 4, tail: 0, onRead: null, p1),
            BuildObservableCoordinator(head: 20, sliceSize: 4, tail: 0, onRead: null, p2),
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
        // DeleteRange at offset 1 and the ledger is genuinely SATURATED, so
        // the offset falls back to the in-memory clamp: every foot-of-loop
        // flush computes a ceiling of minDeferred - 1 == 0, which has not
        // passed the persisted checkpoint of 0, so nothing is persisted. The
        // slice is still SCANNED. The activation is then torn down having
        // banked nothing, and the next one faces the identical window - the
        // #1513 livelock, reached through the one clamp arm #2165 did not
        // retire.
        //
        // WHICH DOOR THIS GOES THROUGH, because it is the whole point of the
        // arm. TryRecordUnresolvedReplayWork has two false returns:
        //
        //     if (cap <= 0)            return false;   // #2165 SWITCHED OFF
        //     if (work.Count >= cap)   return false;   // ledger SATURATED
        //
        // An earlier revision of this arm passed cap 0, which exits at the
        // first and never evaluates saturation at all - so it proved the much
        // weaker "with #2165 disabled, a head-of-window terminal banks
        // nothing". A positive cap reached by real deferrals during the sweep
        // takes the second door, which is the one that matters.
        //
        // HONESTY: cap 1 is still a configuration perturbation, not the
        // deployed setting, and this arm must not be written up as "clamps at
        // deployed settings". What it establishes is that saturation reaches
        // the drop branch and the clamp pins. The branch is cap-RELATIVE
        // (work.Count >= cap), so the deployed cap of 1024 behaves identically
        // at 1025 entries; the arm is small for speed, not because the
        // property is small.
        var (checkpoint, persists, fault) = await RunInterruptedDeferredReplayAsync(
            succeedReads: 2,
            deferredAt: 1,
            maxDurableUnresolvedReplayWork: 1);

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
        // Identical to the arm under test except that the cap is large enough
        // to absorb both deferred offsets, so nothing falls back to the clamp.
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
        // ledger breaks banking". Runs at the SAME positive cap as the arm
        // under test and is refused by the SAME saturation branch; the only
        // variable that moves is where the deferred terminal sits. Its offset
        // of 5 leaves a bankable prefix below it, so the clamp lands at 4 and
        // progress is made.
        var (checkpoint, _, fault) = await RunInterruptedDeferredReplayAsync(
            succeedReads: 2,
            deferredAt: 5,
            maxDurableUnresolvedReplayWork: 1);

        Assert.That(fault, Is.InstanceOf<OperationCanceledException>());
        Assert.That(checkpoint, Is.GreaterThan(0L),
            "A deferred terminal below the scanned prefix must not stop that prefix banking.");
    }

    [Test]
    public async Task A_red_here_means_the_durable_ledger_now_accumulates_and_this_defect_is_live()
    {
        // ESCALATION TRIPWIRE - a red here is NOT "a test broke". It means the
        // pruning invariant has been weakened, so entries the imminent replay
        // is about to RE-READ are being retained instead of dropped. Treat it
        // as a live correctness defect in WAL replay and escalate rather than
        // adjusting this test.
        //
        // The invariant it guards is in RestoreUnresolvedReplayWork:
        //
        //     if (entry.Offset > checkpoint)
        //         continue; // Replay re-reads it; the WAL stays the single source.
        //
        // SCOPE, because this is easy to over-read. The arm guards the prune
        // DIRECTION - above the checkpoint is dropped - and nothing more. It
        // is NOT a guard against the ledger accumulating, because accumulation
        // at or below the checkpoint is legitimate, expected, and the stock
        // route to the cap described on the class. The seeded entries here sit
        // far ABOVE this replay's window precisely so that retaining even one
        // of them is unambiguously the invariant breaking rather than ordinary
        // carry-in.
        //
        // This arm seeds a FULL ledger at the shipping cap and asserts that it
        // still does not starve the flush.
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
