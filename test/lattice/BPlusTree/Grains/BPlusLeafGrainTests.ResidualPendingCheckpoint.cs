using NUnit.Framework;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #3608: a resident, write-idle leaf holding a residual pending replay
/// advance below <c>MaterialiserCheckpointEntries</c> must eventually persist
/// it, without waiting for another <c>SetCheckpointOffsetAsync</c> call.
/// </summary>
/// <remarks>
/// <para>
/// <b>The defect.</b> The checkpoint-coalescing predicate (zero interval, entry
/// threshold, or interval elapsed) was evaluated only inside
/// <c>SetCheckpointOffsetAsync</c>. An advance that arrived inside the interval
/// and below the entry threshold - the last partition an activation replay
/// reconciled, say - stayed pending for as long as the leaf stayed resident and
/// write-idle, because no later advance arrived to re-ask the question. The
/// durable checkpoint, and so the durable pin and the tree's WAL trim floor,
/// froze below the leaf's in-memory position until a teardown persist.
/// </para>
/// <para>
/// <b>The remedy under test.</b> The coverage-lag tick re-evaluates the same
/// predicate and, when it is due, commits the pending advance in its
/// permit-free bank step. A tick inside the window must still flush nothing, or
/// coalescing is defeated.
/// </para>
/// <para>
/// Both fixtures run under the pin-bank leaf's coalescing options
/// (<c>MaterialiserCheckpointInterval</c> one hour, entries one million), so
/// the activation replay's advance to 3 is PENDING and below the entry
/// threshold: exactly the residual shape the issue describes. The window is
/// closed deterministically by ageing the last-persist timestamp rather than by
/// sleeping.
/// </para>
/// </remarks>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// THE regression. With the interval elapsed and no further advance, one
    /// coverage-lag tick must persist the residual advance and raise the durable
    /// pin to it. Pre-fix the tick banked with <c>flushPendingCheckpoint:
    /// false</c>, so persisted stayed 0 and the pin stayed clamped at
    /// <c>min(persisted 0, coverage)</c> = 0.
    /// </summary>
    [Test]
    public async Task OnCoverageLagTimerTickAsync_persists_a_residual_pending_advance_once_the_checkpoint_interval_has_elapsed()
    {
        var (leaf, _) = await ActivatePinBankLeafAsync();
        leaf.Published.Clear();
        var writesBefore = leaf.State.WriteCount;

        leaf.Grain.AgeLastCheckpointPersistForTest(TimeSpan.FromHours(2));
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(3L),
                "THE assertion: the residual advance to 3 must reach the durable checkpoint on the first "
                    + "tick after the interval elapsed. Pre-fix nothing re-evaluated the coalescing predicate, "
                    + "so persisted stayed 0 for as long as the leaf stayed resident.");
            Assert.That(leaf.State.WriteCount, Is.GreaterThan(writesBefore),
                "the commit must be a real durable write, not only an in-memory relabel.");
            Assert.That(leaf.Grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "control: the in-memory position is unchanged by the commit.");
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Does.Contain(3L),
                "the durable pin must rise to min(persisted 3, coverage 3). Pre-fix it was clamped at "
                    + "persisted 0, which is what froze the WAL trim floor.");
            Assert.That(leaf.Batched.All(p => p.PublishedOffset <= Math.Min(p.PersistedCheckpoint, p.Coverage)),
                Is.True,
                "safety: every published pin is clamped by min(persisted, coverage) standing when it was "
                    + "published; the tick never publishes the pending position.");
        });

        // Once committed there is nothing residual: a further tick writes nothing.
        var writesAfterCommit = leaf.State.WriteCount;
        leaf.Grain.AgeLastCheckpointPersistForTest(TimeSpan.FromHours(2));
        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);
        Assert.That(leaf.State.WriteCount, Is.EqualTo(writesAfterCommit),
            "a tick with no pending advance must not persist, however long ago the last persist was.");
    }

    /// <summary>
    /// The coalescing control. Inside the interval, below the entry threshold,
    /// the tick must leave the advance pending: the predicate it re-evaluates is
    /// the one <c>SetCheckpointOffsetAsync</c> applies, and that predicate says
    /// hold. A fix that flushed on every tick passes the regression above and
    /// fails here.
    /// </summary>
    [Test]
    public async Task OnCoverageLagTimerTickAsync_leaves_a_residual_pending_advance_pending_inside_the_checkpoint_interval()
    {
        var (leaf, _) = await ActivatePinBankLeafAsync();
        leaf.Published.Clear();
        var writesBefore = leaf.State.WriteCount;

        await leaf.Grain.OnCoverageLagTimerTickAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(leaf.State.State.ProjectionCheckpointOffset, Is.EqualTo(0L),
                "inside the one-hour interval and below one million entries the advance must stay pending; "
                    + "a tick that commits it defeats checkpoint coalescing.");
            Assert.That(leaf.State.WriteCount, Is.EqualTo(writesBefore),
                "and no checkpoint persist may run.");
            Assert.That(leaf.Grain.GetCurrentCheckpointForPartition(0), Is.EqualTo(3L),
                "control: the advance is still held pending in memory.");
            Assert.That(leaf.Batched.Select(p => p.PublishedOffset), Is.All.LessThanOrEqualTo(0L),
                "with persisted still 0 no pin above 0 may be published.");
        });
    }
}
