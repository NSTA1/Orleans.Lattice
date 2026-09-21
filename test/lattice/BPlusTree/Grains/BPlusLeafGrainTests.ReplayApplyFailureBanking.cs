using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3084: the APPLY half of the replay
/// livelock. Issue #2742 fixed the READ half - a slice that cannot be
/// afforded is narrowed, and when even a single-entry read is refused the
/// partition concedes with its checkpoint already moved, so the next
/// activation faces a strictly shorter gap. That exit is what makes the read
/// side a deferral rather than a dead end, and
/// <c>BPlusLeafGrainTests.ReplayPressureDegradation.cs</c> pins it.
/// <para>
/// The apply side had no such exit. <c>ILeafProjection.Apply</c> was invoked
/// from the per-entry loop inside <c>ReplayPartitionAsync</c> with no guard
/// around it, and the flush that banks the partition's progress
/// (<c>TryFlushRecoveredCeilingAsync</c>) sat AFTER that loop. So a failure
/// raised while applying entry N unwound the whole partition: every entry
/// below N had been applied to the in-memory projection, and every one of
/// them was discarded along with the activation. Net durable progress for the
/// attempt was exactly zero, and the retry re-read the identical slice from
/// the identical offset and failed identically.
/// </para>
/// <para>
/// That zero is what closes the loop described in #3084. A frozen projection
/// checkpoint holds the WAL GC pin, an unreclaimed WAL keeps memory pressure
/// high, and high pressure is what made the entry unapplyable in the first
/// place. The measured shape in the field was 145 executed GC passes with 120
/// blocked, 24 idle, and <b>0 reclaimed</b> - a livelock, not a transient,
/// because the only mechanism that would ease the pressure is the
/// reclamation the failure is blocking.
/// </para>
/// <para>
/// The fix is not to swallow the failure or to skip the offending entry:
/// skipping an entry this leaf owns is silent data loss. It is to bank the
/// prefix that genuinely was applied before letting the failure propagate
/// unchanged. These arms pin all three halves of that - progress is banked,
/// it stops strictly below the entry that failed, and the exception still
/// reaches the caller.
/// </para>
/// <para>
/// The fault is injected through a real production throw site rather than a
/// test-only seam: a committed CRDT-mode Set carrying a typed delta for
/// <see cref="LatticeMergeMode.OrMap"/>, a mode whose shape must be
/// registered explicitly via <c>AddOrMapShape</c> and is not registered in
/// this harness. <c>ApplySet</c> folds the delta, the shape registry cannot
/// resolve it, and <c>LatticeCrdtShapeNotRegisteredException</c> comes out of
/// the same <c>projection.Apply(...)</c> call the field's
/// <c>OutOfMemoryException</c> came out of, on the same line of the same
/// loop - deterministically, and without having to exhaust a real heap to
/// observe anything.
/// </para>
/// <para>
/// An unknown <see cref="MutationKind"/> would have been the shorter
/// injection and is the wrong one: <c>ShouldApplyDuringReplay</c> ends in
/// <c>_ =&gt; false</c>, so such an entry is filtered out before
/// <c>Apply</c> is ever called. It produces a red arm that never reaches the
/// code under test, which is indistinguishable from a real failure by the
/// result alone. The mode above was chosen because it passes that filter.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The offset of the entry that cannot be applied. It is the third of
    /// twelve and sits inside the FIRST four-entry slice, so the entries that
    /// precede it are banked by the fix rather than by an earlier slice
    /// boundary that would have flushed anyway. Without that the arm would
    /// pass for the wrong reason.
    /// </summary>
    private const long UnapplyableOffset = 3;

    /// <summary>
    /// A committed mutation the leaf cannot apply. It is a CRDT-mode Set
    /// carrying a typed delta for <see cref="LatticeMergeMode.OrMap"/>, whose
    /// shape descriptor this harness never registers, so the fold inside
    /// <c>ApplySet</c> cannot resolve it. The kind is <c>Set</c>, so the
    /// entry passes <c>ShouldApplyDuringReplay</c> and genuinely reaches
    /// <c>projection.Apply</c>.
    /// </summary>
    private static CommitLogSliceEntry UnapplyableEntry(long offset, string key) =>
        new(offset, new LatticeMutation
        {
            TreeId = ResumableTreeId,
            Kind = MutationKind.Set,
            Key = key,
            Mode = LatticeMergeMode.OrMap,
            Delta = Encoding.UTF8.GetBytes("typed-delta-with-no-registered-shape"),
            Timestamp = new HybridLogicalClock { WallClockTicks = 100 },
        });

    /// <summary>
    /// Twelve committed Sets with the entry at
    /// <see cref="UnapplyableOffset"/> swapped for one that cannot be
    /// applied.
    /// </summary>
    private static CommitLogSliceEntry[] EntriesWithUnapplyableAt(long poisonOffset, int count = 12) =>
        Enumerable.Range(1, count)
            .Select(i => i == poisonOffset
                ? UnapplyableEntry(i, $"k{i:D2}")
                : Set(i, $"k{i:D2}"))
            .ToArray();

    [Test]
    public void A_replay_that_fails_mid_slice_still_banks_the_entries_it_applied()
    {
        // THE ISSUE. Offsets 1 and 2 are applied to the projection, offset 3
        // throws. Before the fix the throw escaped the per-entry loop, skipped
        // the foot-of-slice flush entirely, and the activation unwound with
        // the checkpoint still at 0 - so the next activation re-read the
        // identical slice from the identical offset. Banking nothing is what
        // makes the retry a repeat rather than a step, and a repeat is what
        // pins the WAL segment that the memory pressure depends on being
        // pinned.
        var entries = EntriesWithUnapplyableAt(UnapplyableOffset);
        var coord = BuildChunkingCoordinator(head: 12, sliceSize: 4, tail: 0, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord, store.Stub, reclassifyEveryN: 1);

        Assert.ThrowsAsync<LatticeCrdtShapeNotRegisteredException>(
            async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));

        Assert.That(state.State.ProjectionCheckpointOffset, Is.GreaterThan(0L),
            "A replay that failed while applying an entry must leave the checkpoint ahead of where "
            + "it found it. Banking nothing means the next activation repeats this one exactly, and "
            + "a checkpoint that never advances is what holds the WAL GC pin that keeps the memory "
            + "pressure this failure came from.");
    }

    [Test]
    public void The_banked_checkpoint_stops_strictly_below_the_entry_that_failed()
    {
        // The counterweight to the arm above, and the reason this fix is not
        // simply "skip the bad entry". The checkpoint is a watermark of how
        // far this leaf has got; advancing it past an entry the leaf owns but
        // did not apply is silent data loss, which is strictly worse than the
        // stall it would cure. Banking must stop at the last entry that was
        // fully applied.
        //
        // Asserting GreaterThan(0) alone would be satisfied by a fix that
        // banked the whole slice, so without this arm the pair is not a
        // specification of the correct value - only of a non-zero one.
        var entries = EntriesWithUnapplyableAt(UnapplyableOffset);
        var coord = BuildChunkingCoordinator(head: 12, sliceSize: 4, tail: 0, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord, store.Stub, reclassifyEveryN: 1);

        Assert.ThrowsAsync<LatticeCrdtShapeNotRegisteredException>(
            async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));

        Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(UnapplyableOffset - 1),
            "The banked checkpoint must be exactly the last offset that was fully applied. Lower "
            + "discards progress that really was made; at or above the failed offset licenses a "
            + "resume that skips an entry this leaf owns.");
    }

    [Test]
    public void The_failure_still_propagates_after_the_prefix_is_banked()
    {
        // Banking must not turn a failed partition into a successful
        // activation. A leaf that did not finish its replay must not serve
        // reads, so the original exception has to reach the caller unchanged -
        // the same contract Materialiser_propagates_apply_failures pins for
        // the read side. Swallowing it here would trade a visible stall for
        // silently incomplete data, which is the one outcome worse than the
        // livelock.
        var entries = EntriesWithUnapplyableAt(UnapplyableOffset);
        var coord = BuildChunkingCoordinator(head: 12, sliceSize: 4, tail: 0, entries);
        var store = new InMemorySnapshotStore();
        var state = NewResumableState();

        var (grain, _) = BuildResumableLeaf(state, coord, store.Stub, reclassifyEveryN: 1);

        var ex = Assert.ThrowsAsync<LatticeCrdtShapeNotRegisteredException>(
            async () => await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None));

        Assert.That(ex!.Message, Does.Contain("No CrdtShape is registered"),
            "The exception the caller sees must still be the one the apply raised, not a wrapper "
            + "that hides which entry could not be applied.");
    }
}
