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
/// <c>#2746</c> and its reproduction ships here <c>[Ignore]</c>d; the four
/// arms that DO run are the controls that isolate it, the tripwire that
/// bounds it, and the measured bound on its reachability.
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
/// said the cap was "only reachable WITHIN one activation". Its REASONING was
/// wrong and its CONCLUSION turned out to be right, which is the most
/// dangerous combination to leave undocumented, so both halves are corrected
/// here rather than quietly deleted.
/// </para>
/// <para>
/// The reasoning was wrong because the prune alone does not bound anything:
/// it holds only for a leaf pinned at checkpoint 0, where EVERY entry is above
/// the checkpoint and the ledger does prune to empty. Above checkpoint 0 the
/// prune KEEPS entries, and the two kinds it keeps are asymmetric - a kept
/// deferred TERMINAL is handed to pass 2 and drains, but a kept unresolved
/// PREPARE is re-applied and nothing reaps it (#2304). Read off the prune
/// alone, a leaf therefore looks able to inherit a ledger already at the cap
/// and starve on its first deferred terminal, reaching this defect at the
/// SHIPPING cap with no configuration deviation.
/// </para>
/// <para>
/// The conclusion survives anyway, because the prune is not the only gate.
/// <c>ReplayWalSinceCheckpointAsync</c> passes a <c>checkpointOverride</c>,
/// and a cold-start cache-empty rebuild sets it to <c>-1</c> for EVERY
/// partition, since the rebuild covers the full readable window of every
/// partition. The restore resolves <c>checkpointOverride ?? persisted</c>, so
/// on a cold rebuild the ENTIRE ledger is discarded - including entries far
/// below the persisted checkpoint that the prune would have kept. A leaf that
/// dies during replay, which is precisely the case #2746 is about,
/// re-activates with an empty cache by construction. Carry-in therefore cannot
/// saturate the cap on the activation that matters, and accumulation WITHIN
/// one activation is the only route to the refusal at the deployed cap.
/// <see cref="A_cold_cache_rebuild_discards_the_whole_ledger_which_bounds_this_defects_reachability"/>
/// pins that, and is the only arm here that was arrived at by EXECUTION
/// refuting a reading rather than by reading alone.
/// </para>
/// <para>
/// WHAT FILLS THE LEDGER WITHIN ONE ACTIVATION. An earlier revision said this
/// needed a tree that runs sagas, and that the candidate nominated for it -
/// <c>repo-context-vector-index</c> - carries no prepares at all, so the stock
/// route and that candidate were "mutually exclusive". That is RETRACTED. The
/// deferral branch is not saga-only: <see cref="MutationKind.DeleteRange"/>
/// sits in the deferral condition alongside the transaction terminals, and
/// <c>LatticeVectorIndexStore.DeletePrefixAsync</c> issues
/// <c>DeleteRangeAsync</c> as its PRIMARY arm whenever the prefix has a finite
/// upper bound - the per-key walk is only the unbounded fallback. So a
/// prepare-free tree fills its own ledger with range-delete terminals: on a
/// multi-partition tree only the partition absorbed LAST drains inline
/// (#1831), so every other partition defers, and an activation torn down in
/// pass 1 never reaches the pass-2 drain that would strike them off.
/// </para>
/// <para>
/// Prepares remain a second, independent filler:
/// <c>EnsureUnresolvedPrepareRecorded</c> is UNCAPPED by design (#2183) yet
/// shares one list with the capped terminal writer, so on a tree that does run
/// sagas they consume the budget that bounds terminals. Neither route has been
/// shown to FIRE on a live tree - what is established is the mechanism, not a
/// count - and this file should not be read as claiming otherwise.
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
/// terminal offered to it and never increments anything. <c>#2757</c> is
/// raised against both halves: it meters the drop site and makes that
/// threshold inclusive.
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
    /// Three partitions, because two cannot reach the FREEZE at a positive
    /// cap. Pass 1 sweeps by backlog ASCENDING (#2089) and only the partition
    /// absorbed LAST is drain-eligible, so with two partitions the one whose
    /// window opens with a range delete is either swept FIRST, where the
    /// ledger is still empty and its offset records fine, or swept LAST, where
    /// the range delete drains inline (#1831). Only a MIDDLE partition is
    /// neither, so a third is required.
    /// <para>
    /// An earlier revision of this note said "neither reaches the REFUSAL",
    /// which is false, and the distinction it elided is the one a reader of
    /// this fixture most needs. A REFUSAL (the saturation branch at
    /// BPlusLeafGrain.DurableReplayWork.cs:126) and a FREEZE (the ceiling
    /// clamping to at or below the checkpoint at
    /// BPlusLeafGrain.Activation.cs:3117, so nothing banks) are different
    /// events, and the first does not imply the second. A single first-swept
    /// partition carrying cap+1 range deletes DOES reach the refusal off its
    /// own deferrals - measured at cap 2 with four range deletes, and at cap 1
    /// with two.
    /// </para>
    /// <para>
    /// What those measurements showed is that such a partition BANKS AT
    /// EXACTLY cap rather than freezing, and the reason generalises to every
    /// partition that fills the ledger itself: filling it requires at least
    /// cap of that partition's OWN offsets to have been ACCEPTED first, so its
    /// first refusal necessarily lands at offset cap+1. That gives
    /// minDeferred = cap+1, hence ceiling = cap >= 1 > 0, which clears the
    /// :3117 guard. The freeze needs minDeferred == 1 - the window must OPEN
    /// on a refusal - which requires the ledger to be FULL ON ARRIVAL, which
    /// requires a DIFFERENT, EARLIER-SWEPT partition to have filled it. So the
    /// third partition is required by an invariant, not by an enumeration of
    /// cases, and a fixture that reaches only the refusal would come back
    /// green while pinning nothing about the freeze.
    /// </para>
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
        int preSeededLedgerEntries = 0,
        long? seededLedgerPartitionCheckpoint = null,
        FakePersistentState<LeafNodeState>? existingState = null)
    {
        var state = existingState ?? NewFlushCeilingState();

        // Reproduces a durable ledger carried in from earlier activations.
        // Whether these entries SURVIVE the restore is decided entirely by
        // seededLedgerPartitionCheckpoint, and that is the point of the
        // parameter: the seed itself is identical in both directions, so the
        // prune direction is the single variable between
        // A_red_here_means_the_durable_ledger_now_accumulates_and_this_defect_is_live
        // (checkpoint left unset, every entry above it, all DROPPED) and
        // A_replay_whose_window_opens_with_a_deferred_terminal_banks_progress_at_the_shipping_cap
        // (checkpoint set above them, all KEPT).
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

            if (seededLedgerPartitionCheckpoint is long seededCheckpoint)
            {
                // Partition 1 only. Partition 0 is deliberately left on the
                // unassigned scalar so it still resolves to the "nothing
                // applied" sentinel and its window still OPENS at offset 1,
                // which is the position the defect needs.
                state.State.ProjectionCheckpointOffsetsByPartition =
                    [0L, seededCheckpoint, -1L];
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

    [Test]
    public async Task A_cold_cache_rebuild_discards_the_whole_ledger_which_bounds_this_defects_reachability()
    {
        // REACHABILITY BOUND, and it exists because an earlier revision of
        // this file asserted the opposite. That revision reasoned that since
        // RestoreUnresolvedReplayWork KEEPS entries at or below their
        // partition checkpoint, an activation could inherit a ledger already
        // at the cap and starve on its very first deferred terminal - a route
        // to the defect at the SHIPPING cap with no configuration deviation.
        // That reasoning was read off the prune and never executed. Executing
        // it refutes it.
        //
        // The prune is not the only gate. ReplayWalSinceCheckpointAsync passes
        // a checkpointOverride, and a cold-start cache-empty rebuild sets it
        // to -1 for EVERY partition, "because the cache rebuild covers the
        // full readable window of every partition"
        // (BPlusLeafGrain.Activation.cs). The restore resolves
        // checkpointOverride ?? persisted, so on a cold rebuild every entry is
        // above the effective checkpoint and the ENTIRE ledger is discarded -
        // including entries far below the persisted checkpoint that the
        // ordinary prune would have kept.
        //
        // That is the bound: carry-in cannot saturate the cap on a cold
        // activation, so at the deployed cap the ONLY route to the refusal is
        // accumulation WITHIN a single activation. It matters because a leaf
        // that dies during replay - the case #2746 is about - re-activates
        // with an empty cache by construction, which is exactly the condition
        // that discards the ledger.
        //
        // HOW THIS ARM AVOIDS THE TRAP THAT HID IT. The obvious test is to run
        // two activations and check the ledger is non-empty afterwards. That
        // is VACUOUS: the second activation re-reads the same window and
        // re-records the same offsets, so a ledger that was discarded and then
        // rebuilt from scratch is indistinguishable from one that survived. An
        // earlier draft of this arm passed for precisely that reason. The
        // seeded offsets here are 100_000+, which appear in NO partition's WAL
        // window, so they cannot be re-recorded and their presence or absence
        // is unambiguous.
        //
        // The partition-1 checkpoint is deliberately set ABOVE every seeded
        // offset, so the ordinary prune would KEEP all of them. The only thing
        // that can drop them is the override. That is what makes this arm a
        // test of the override specifically rather than of the prune - the
        // tripwire arm above already covers the prune, using the identical
        // seed with the checkpoint left unset.
        var state = NewFlushCeilingState();
        var (_, _, fault) = await RunInterruptedDeferredReplayAsync(
            succeedReads: 2,
            deferredAt: 1,
            maxDurableUnresolvedReplayWork: LatticeOptions.DefaultMaxDurableUnresolvedReplayWork,
            preSeededLedgerEntries: LatticeOptions.DefaultMaxDurableUnresolvedReplayWork,
            seededLedgerPartitionCheckpoint: 100_000L + LatticeOptions.DefaultMaxDurableUnresolvedReplayWork,
            existingState: state);

        Assert.That(fault, Is.InstanceOf<OperationCanceledException>());

        var seededSurvivors = (state.State.UnresolvedReplayWork ?? [])
            .Where(e => e.Offset >= 100_000L)
            .Select(e => e.Offset)
            .ToList();

        // Assert on the count, not the list: a failure here retains up to the
        // whole cap, and dumping 1,024 offsets buries the explanation that
        // follows it.
        Assert.That(seededSurvivors.Count, Is.Zero,
            "A cold-cache rebuild retained durable ledger entries. Every seeded offset sits at or below "
            + "its partition's persisted checkpoint, so the ordinary prune would keep them; the "
            + "cold-start override is what must discard them, and it is what bounds this defect's "
            + "reachability at the shipping cap. If entries now survive a cold rebuild, a ledger can be "
            + "inherited at capacity and the deferred-terminal clamp of issue #2746 becomes reachable "
            + "with no configuration deviation at all. Re-derive that bound before trusting it. "
            + $"Survivors: {seededSurvivors.Count}, first few: "
            + $"[{string.Join(", ", seededSurvivors.Take(5))}].");
    }

    /// <summary>
    /// Drives the liveness-priority admission of issue #2746 directly, on a
    /// window whose non-last partition opens with THREE consecutive deferred
    /// range deletes against a ledger that is already saturated when it is
    /// swept.
    /// <para>
    /// Every other arm in this fixture asserts on the resulting CHECKPOINT,
    /// which is the end-to-end effect. This one asserts on the durable ledger's
    /// CONTENTS, because the checkpoint cannot distinguish the two clauses the
    /// fix is made of - it moves identically whether the head offset alone was
    /// admitted or every offset was. Asserting on membership rather than on an
    /// aggregate is deliberate: an aggregate (a count, a maximum, a "did it
    /// advance") is satisfied by too many wrong implementations to falsify
    /// either clause on its own.
    /// </para>
    /// </summary>
    private static async Task<(List<(int Partition, long Offset)> Ledger, Exception? Fault)>
        RunSaturatedHeadOfWindowReplayAsync(int cap)
    {
        var state = NewFlushCeilingState();

        // Partition 0: the one under test. Swept SECOND, so it defers rather
        // than draining inline, and its window opens on a deferred terminal.
        // Offsets 1, 2 and 3 are all range deletes so the arm can tell the
        // head offset apart from the ones behind it.
        var p0 = new CommitLogSliceEntry[12];
        for (var i = 1; i <= 12; i++)
            p0[i - 1] = i <= 3 ? FlushDeleteRange(i) : FlushSet(i, $"h{i:D2}");

        // Partition 1: swept FIRST (smallest backlog). Its own deferred range
        // delete is what saturates the cap before partition 0 is swept at all,
        // which is the condition the defect needs and the reason this shape
        // takes three partitions rather than two.
        var p1 = new CommitLogSliceEntry[8];
        for (var i = 1; i <= 8; i++)
            p1[i - 1] = i == 4 ? FlushDeleteRange(i) : FlushSet(i, $"g{i:D2}");

        // Partition 2: swept LAST (largest backlog), so it is the single
        // drain-eligible partition and contributes nothing to the ledger.
        var p2 = new CommitLogSliceEntry[20];
        for (var i = 1; i <= 20; i++)
            p2[i - 1] = FlushSet(i, $"f{i:D2}");

        using var cts = new CancellationTokenSource();
        ILeafReplayCoordinatorGrain[] coordinators =
        [
            BuildObservableCoordinator(
                head: 12,
                sliceSize: 4,
                tail: 0,
                onRead: read =>
                {
                    // Tear down at the SECOND read, so partition 0's first
                    // slice (offsets 1-4, carrying all three range deletes) is
                    // fully absorbed first. Cancelling at the first read would
                    // abort before the slice is processed and the ledger would
                    // never see the offers at all. The ledger is only
                    // observable BECAUSE the activation never reaches pass 2 -
                    // a completed replay resolves every deferred terminal and
                    // empties the ledger again.
                    if (read >= 2)
                        cts.Cancel();
                },
                p0),
            BuildObservableCoordinator(head: 8, sliceSize: 4, tail: 0, onRead: null, p1),
            BuildObservableCoordinator(head: 20, sliceSize: 4, tail: 0, onRead: null, p2),
        ];

        var store = new InMemorySnapshotStore();
        var grain = BuildFlushCeilingLeaf(
            state,
            coordinators,
            store.Stub,
            maxDurableUnresolvedReplayWork: cap);

        Exception? fault = null;
        try
        {
            await ((IGrainBase)grain).OnActivateAsync(cts.Token);
        }
        catch (Exception ex)
        {
            fault = ex;
        }

        var ledger = (state.State.UnresolvedReplayWork ?? [])
            .Select(e => (e.Partition, e.Offset))
            .ToList();

        return (ledger, fault);
    }

    [Test]
    public async Task A_saturated_ledger_admits_the_head_of_window_terminal_and_refuses_the_ones_behind_it()
    {
        // THE TWO CLAUSES OF ISSUE #2746'S FIX, each falsifiable on its own
        // from this single arm:
        //
        //   ADMISSION - the head offset is recorded even though the ledger is
        //   already at the cap, because refusing it would clamp the ceiling to
        //   offset - 1, which is exactly where the window opened, retiring no
        //   entry at all. The next activation would re-read the identical
        //   window and freeze identically. Revert this clause and the "contains
        //   (0, 1)" assertion below goes red.
        //
        //   BOUND - offsets 2 and 3 are still REFUSED, because by then an entry
        //   has been consumed below them, so the clamp lands at a ceiling that
        //   does retire work and the partition still makes progress. The cap
        //   keeps its full force for every offer that is not at the head.
        //   Revert this clause - admit unconditionally once past the cap - and
        //   the "does not contain (0, 2) / (0, 3)" assertions go red while the
        //   admission assertion stays green.
        //
        // The two assertions therefore fail on DISJOINT perturbations, which is
        // what makes the arm a test of the fix rather than of its effect.
        const int cap = 1;
        var (ledger, fault) = await RunSaturatedHeadOfWindowReplayAsync(cap);

        Assert.That(fault, Is.InstanceOf<OperationCanceledException>(),
            "The teardown must actually have ended the activation before pass 2, or the ledger would "
            + "have been drained by the deferred terminals resolving and this arm would prove nothing.");

        Assert.Multiple(() =>
        {
            Assert.That(ledger, Does.Contain((1, 4L)),
                "Partition 1 is swept first and its deferred range delete is what saturates the cap. "
                + "If it is absent the ledger was never saturated when partition 0 was swept, so the "
                + "rest of this arm is not exercising the saturation branch at all.");

            Assert.That(ledger, Does.Contain((0, 1L)),
                "The head-of-window deferred terminal was refused by the saturated ledger. Refusing it "
                + "clamps partition 0's flush ceiling to offset 0, which is where its window already "
                + "opened, so the activation banks nothing and the next one re-reads the identical "
                + "window - the issue #2746 freeze. It must be admitted past the cap precisely because "
                + "it is the one offer whose refusal cannot be recovered from.");

            Assert.That(ledger, Does.Not.Contain((0, 2L)),
                "Offset 2 is NOT at the head of the window - offset 1 precedes it - so refusing it "
                + "leaves a ceiling that retires real work and the partition still progresses. "
                + "Admitting it means the cap has stopped bounding the ledger at all, which is the "
                + "unbounded-growth hazard the cap exists to prevent.");

            Assert.That(ledger, Does.Not.Contain((0, 3L)),
                "Offset 3 is likewise behind the head and must still be refused. Two admissions in one "
                + "partition would mean the admission predicate is not self-limiting, and the "
                + "'at most one row per partition per activation' bound that makes this fix safe "
                + "without any threshold would no longer hold.");

            Assert.That(ledger, Has.Count.EqualTo(cap + 1),
                "The ledger must exceed the cap by exactly one row: the single head-of-window "
                + "admission. This is the whole safety argument for having no tunable - the overshoot "
                + "is structurally bounded by one row per partition per activation, each one bought "
                + $"with a strictly shorter window next time. Ledger was: [{string.Join(", ", ledger)}].");
        });
    }
}