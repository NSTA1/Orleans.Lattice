using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4017: a failed checkpoint persist must not leave the leaf advertising
/// a durable materialiser pin the WAL GC may trim to.
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect.</b> <c>FlushPendingCheckpointAsync</c> committed the pending
/// advance into <c>state.State</c> and dropped
/// <c>_pendingCheckpointOffsetsByPartition</c> BEFORE awaiting
/// <c>PersistAsync</c>. A persist that throws therefore left the activation's
/// in-memory checkpoint ahead of the durable one with the pending record
/// destroyed, so nothing could ever re-persist it.
/// </para>
/// <para>
/// That matters because <c>GetPersistedCheckpointForPartition</c> - which reads
/// <c>state.State</c> and is the single accessor every durability decision goes
/// through - then reports an offset that was never written. Issue #3476 clamped
/// the published pin to exactly that accessor, so the clamp went on holding
/// while the value it clamps to had stopped being true. The pin store merges
/// offsets by monotonic max and can never take one back, the WAL GC's offset
/// floor is a minimum over those pins, and the next activation replays from the
/// checkpoint that actually reached storage: the trimmed prefix is gone and the
/// replay throws <c>LeafProjectionStaleException</c>.
/// </para>
/// <para>
/// <b>The enabling condition is real.</b> Issue #2419's SQLite write convoy
/// fails a checkpoint-advance write terminally ("Attempt 1; retrying: False"),
/// which is what <see cref="Fakes.FakePersistentState{T}.ThrowOnWrite"/> models
/// here. Retrying that write would make the failure rarer; it cannot establish
/// the invariant, because a write can always ultimately fail. The durability
/// contract has to hold without assuming storage succeeds.
/// </para>
/// <para>
/// <b>The observable.</b> The durable checkpoint is the value carried by the
/// last <em>successful</em> <c>WriteStateAsync</c> (captured through
/// <c>OnWriteState</c>), never <c>state.State</c> - reading the latter is the
/// very conflation under test. Each published pin is paired with the leaf state
/// behind it at the instant it published, because the invariant relates two
/// values at one moment and a later persist would otherwise mask an over-report
/// the pin store has already merged.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The terminal storage failure shape issue #2419 records: the busy window
    /// is exhausted and the write is NOT retried, so the advance is simply lost.
    /// </summary>
    private static InvalidOperationException UnretriedCheckpointWriteFailure() =>
        new("Grain storage write failed on a SQLite lock (error 5, extended 5) after "
            + "15016 ms against a 15000 ms busy window (exhausted). Attempt 1; retrying: False.");

    /// <summary>
    /// Brings a coalescing leaf to the exact pre-incident state: a pending
    /// advance to <c>3</c> over a durable checkpoint of <c>0</c>, with snapshot
    /// coverage already restamped to <c>3</c> by the issue #3224 coverage-lag
    /// tick so the coverage arm of the pin's <c>min</c> no longer holds it down.
    /// Only the persisted-checkpoint arm is left, which is the arm under test.
    /// </summary>
    private static async Task<(BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State,
        List<PinPublication> Published, List<long> DurableWrites)> CreateLeafWithUnpersistableAdvanceAsync()
    {
        var wal = new GrowingWal();
        var (grain, state, published, durableWrites) = CreateCoalescingLeafWithPinCapture(wal.Coordinator);

        wal.GrowTo(3);
        await ActivateAsync(grain);
        await grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Clock, Is.GreaterThan(HybridLogicalClock.Zero),
                "precondition: the replay applied real entries, so the pin publishers are live. "
                    + "A Zero clock returns before resolving any pin and the fixture would assert "
                    + "nothing.");
            Assert.That(durableWrites, Has.None.GreaterThan(0L),
                "precondition: no checkpoint above 0 has ever reached storage, so 0 - the "
                    + "offset the rehydrated snapshot covers - is unambiguously the durable "
                    + "checkpoint this fixture measures against.");
            Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "precondition: the coalescing options held the replayed advance PENDING at 3.");
            Assert.That(grain.DurableSnapshotCoverageForPartition(0), Is.EqualTo(3L),
                "precondition: coverage was restamped from the pending checkpoint (#3224), so "
                    + "the coverage arm of min(persisted, covered) no longer protects anything. "
                    + "Without this the pin is held at 0 by coverage and the fixture cannot redden.");
        });

        return (grain, state, published, durableWrites);
    }

    /// <summary>
    /// THE fixture. After a checkpoint persist fails terminally, no durable pin
    /// the leaf publishes may exceed the checkpoint that actually reached
    /// storage.
    /// <para>
    /// Pre-fix the commit had already written 3 into <c>state.State</c>, so
    /// <c>ResolveDurablePinForPartition</c> resolved <c>min(3, 3) == 3</c> and
    /// published a trim entitlement three entries above anything durable. That
    /// is the WAL GC trimming past a live leaf's persisted checkpoint with no
    /// covering snapshot - issue #4017's invariant violation - reached without
    /// any operator action.
    /// </para>
    /// </summary>
    [Test]
    public async Task Failed_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint()
    {
        var (grain, state, published, durableWrites) = await CreateLeafWithUnpersistableAdvanceAsync();

        published.Clear();
        durableWrites.Clear();
        state.ThrowOnWrite = UnretriedCheckpointWriteFailure();

        Assert.That(
            async () => await ((ILeafProjection)grain).FlushCheckpointAsync(CancellationToken.None),
            Throws.InvalidOperationException,
            "control: the persist must actually fail, or nothing diverges and the fixture "
                + "asserts nothing.");

        Assert.That(durableWrites, Is.Empty,
            "control: the failed flush wrote nothing durable, so the durable checkpoint is "
                + "still the 0 established in the preconditions.");

        // The publisher every other publisher routes through. It does not
        // persist, so whatever it publishes is resolved purely from the leaf's
        // own view of its persisted checkpoint.
        await grain.FlushDurableMaterialiserFrontierAsync();

        Assert.Multiple(() =>
        {
            Assert.That(published, Is.Not.Empty,
                "control: the flush must publish, or the assertions below pass vacuously.");
            Assert.That(published.Select(p => p.PublishedOffset), Is.All.LessThanOrEqualTo(0L),
                "THE assertion. The durable checkpoint is 0, so no pin may exceed 0. Pre-fix "
                    + "the failed persist left state.State holding 3 and the leaf published 3: "
                    + "the WAL GC's offset floor rises to 3, the prefix is trimmed, and the next "
                    + "activation replays from the durable 0 into a trimmed WAL and latches "
                    + "LeafProjectionStaleException.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
                "the root cause, asserted directly: a persist that threw must leave the "
                    + "in-memory checkpoint where it was. Pre-fix it held the never-written 3, "
                    + "which every durability decision then read through "
                    + "GetPersistedCheckpointForPartition as though it were durable.");
        });
    }

    /// <summary>
    /// The liveness half, and the reason the fix restores the pending map rather
    /// than merely rewinding <c>state.State</c>. Rolling the checkpoint back
    /// while leaving the advance discarded would be safe but would silently drop
    /// it: the leaf would never re-persist offsets 1..3, its pin would sit at 0
    /// for the rest of the activation, and the tree's WAL floor would be held
    /// there with nothing able to lift it.
    /// </summary>
    [Test]
    public async Task Failed_checkpoint_persist_retains_the_pending_advance_for_the_next_flush()
    {
        var (grain, state, published, durableWrites) = await CreateLeafWithUnpersistableAdvanceAsync();

        durableWrites.Clear();
        state.ThrowOnWrite = UnretriedCheckpointWriteFailure();

        Assert.That(
            async () => await ((ILeafProjection)grain).FlushCheckpointAsync(CancellationToken.None),
            Throws.InvalidOperationException,
            "control: the first flush fails.");

        Assert.That(grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
            "the advance must survive the failure as PENDING - the leaf still holds entries "
                + "1..3 in cache, so the work is not lost, only unpersisted.");

        published.Clear();

        // ThrowOnWrite is single-shot, so this flush is the storage recovering.
        await ((ILeafProjection)grain).FlushCheckpointAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(durableWrites, Does.Contain(3L),
                "the retry must persist the advance the failed flush was carrying. Without "
                    + "restoring the pending map there is nothing left to flush and the "
                    + "checkpoint is stuck at 0 for the life of the activation.");
            Assert.That(published.Select(p => p.PublishedOffset), Is.All.LessThanOrEqualTo(3L),
                "and the pin may now reach 3, because 3 is durable.");
            Assert.That(published.Select(p => p.PublishedOffset), Does.Contain(3L),
                "the floor genuinely lifts once the write lands, so the failure costs bounded "
                    + "retention rather than a permanently pinned WAL.");
        });
    }

    /// <summary>
    /// The second commit site, and the more dangerous of the two. The graceful
    /// deactivation barrier restates the commit sequence verbatim rather than
    /// routing through <c>FlushPendingCheckpointAsync</c> (its own doc comment
    /// mandates the two stay in step), so a fix applied to one and not the other
    /// leaves the bug live on this path - and the fixtures above, which drive
    /// only the interface flush, would not notice.
    /// <para>
    /// This path is the worse one because of WHEN it runs. The pin is published
    /// by the frontier barrier moments before the activation is destroyed, so
    /// the in-memory rows the un-persisted checkpoint was speaking for disappear
    /// immediately after the WAL GC has been authorised to trim them. That is
    /// exactly the window the runtime's own stale-leaf diagnostic warns about:
    /// "the live activation may hold the only copy of writes in the trimmed
    /// range ... BEFORE this activation is recycled or the silo restarts".
    /// </para>
    /// </summary>
    [Test]
    public async Task Failed_deactivation_checkpoint_persist_publishes_no_durable_pin_past_the_last_durably_written_checkpoint()
    {
        var (grain, state, published, durableWrites) = await CreateLeafWithUnpersistableAdvanceAsync();

        published.Clear();
        durableWrites.Clear();
        state.ThrowOnWrite = UnretriedCheckpointWriteFailure();

        // Drives the real barrier sequence: the checkpoint-flush barrier commits
        // and faults, is contained, and the frontier-pin barrier then publishes.
        await DeactivateLeafAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(durableWrites, Has.None.GreaterThan(0L),
                "control: the teardown persist failed, so no checkpoint above the durable 0 "
                    + "ever reached storage on this path either.");
            Assert.That(published, Is.Not.Empty,
                "control: the deactivation must still publish a pin, or the assertion below "
                    + "passes vacuously and would hold against a reverted fix.");
            Assert.That(published.Select(p => p.PublishedOffset), Is.All.LessThanOrEqualTo(0L),
                "THE assertion for the deactivation commit site. Pre-fix the barrier left "
                    + "state.State holding the never-written 3 and published it, handing the WAL "
                    + "GC a trim entitlement over a prefix whose only copy was the cache of an "
                    + "activation that is being torn down in the same breath.");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
                "and the in-memory checkpoint must be back where it was, for the same reason "
                    + "as on the flush path.");
        });
    }
}
