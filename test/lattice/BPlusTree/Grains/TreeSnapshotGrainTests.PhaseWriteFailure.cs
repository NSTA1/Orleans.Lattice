using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The per-phase half of the Class B "persisted / in-memory divergence on write
/// failure" guard on <see cref="TreeSnapshotGrain"/>.
/// <para>
/// The sibling <c>WriteFailure</c> fixture covers the three whole-operation
/// entry points (initiate, complete, abort). Every remaining persist in the
/// grain sits inside the phase machine, and each has its own snapshot-and-revert
/// arm: the Lock-to-Copy flip, the two Copy arms (a bounded pass that yielded
/// and a pass that finished the shard), the Unmark advance, the retry-budget
/// bump, the ShadowBegin-to-Copy flip, and the online bulk-drain cursor advance.
/// </para>
/// <para>
/// Note the interaction with the outer <c>catch</c> in
/// <c>ProcessCurrentPhaseAsync</c>: an inner persist failure is reverted, then
/// rethrown into that handler, which burns one retry and persists again. So a
/// test driven through <c>ProcessNextPhaseAsync</c> observes the reverted fields
/// plus <c>ShardRetries == 1</c>, and does not see the exception - the retry
/// budget swallowed it. Each test therefore asserts the reverted field
/// explicitly rather than only that the call threw, because "it threw" would
/// pass just as happily if nothing were reverted.
/// </para>
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private static FakePersistentState<TreeSnapshotState> SeededState(
        SnapshotPhase phase,
        SnapshotMode mode,
        int nextShardIndex = 0,
        int shardRetries = 0,
        int shardCount = ShardCount,
        string? copyCursorKey = null) =>
        new()
        {
            State = new TreeSnapshotState
            {
                InProgress = true,
                Phase = phase,
                Mode = mode,
                NextShardIndex = nextShardIndex,
                ShardRetries = shardRetries,
                ShardCount = shardCount,
                CopyCursorKey = copyCursorKey,
                DestinationTreeId = DestTreeId,
                OperationId = "op-phase-write-failure",
            },
        };

    [Test]
    public void LockSourceShards_reverts_the_copy_flip_when_WriteStateAsync_throws()
    {
        var state = SeededState(SnapshotPhase.Lock, SnapshotMode.Offline, shardRetries: 2);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        // Called directly, so there is no outer retry handler to swallow it.
        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.LockSourceShardsAsync());

        // Without the revert, the activation would report Phase=Copy while disk
        // still holds Lock: RunSnapshotPassAsync would skip re-locking the
        // source shards and start draining a tree that was never quiesced.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Lock));
            Assert.That(state.State.ShardRetries, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task A_yielded_online_copy_reverts_the_resume_cursor_when_WriteStateAsync_throws()
    {
        // Five leaves at two per pass, so the first pass yields with a cursor.
        var h = CreateCopyingSnapshot(leafCount: 5, leavesPerPass: 2, SnapshotMode.Online);
        h.State.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            // The copy itself ran - otherwise the revert assertion below would
            // be vacuous, passing because nothing ever set the cursor.
            Assert.That(h.MergedKeys, Has.Count.EqualTo(2));

            // Without the revert the cursor would hold SnapshotLeafResumeKey(2)
            // while disk still holds null, so a reactivation would resume from
            // leaf 0 and re-copy work this activation believes it has passed.
            Assert.That(h.State.State.CopyCursorKey, Is.Null);
            Assert.That(h.State.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(0));

            // The outer handler burned one retry on the rethrow.
            Assert.That(h.State.State.ShardRetries, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_completed_offline_copy_reverts_the_unmark_advance_when_WriteStateAsync_throws()
    {
        // Offline copies are unbounded, so one pass finishes the shard and the
        // grain flips Copy -> Unmark.
        var h = CreateCopyingSnapshot(leafCount: 2, leavesPerPass: 8, SnapshotMode.Offline);
        h.State.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.MergedKeys, Is.Not.Empty, "the copy must have completed for the flip to be attempted");

            // Without the revert the activation would sit in Unmark while disk
            // holds Copy, so a reactivation would re-run the copy against a
            // destination shard the bulk loader refuses to load twice.
            Assert.That(h.State.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(0));
            Assert.That(h.State.State.CopyCursorKey, Is.Null);
            Assert.That(h.State.State.ShardRetries, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_unmark_pass_reverts_the_shard_advance_when_WriteStateAsync_throws()
    {
        var state = SeededState(SnapshotPhase.Unmark, SnapshotMode.Offline);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        await grain.ProcessNextPhaseAsync();

        // The unmark side effect is deliberately not reverted (it is idempotent),
        // so assert it happened and that only the cursor flip was rolled back.
        await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0").Received(1).UnmarkDeletedAsync();
        Assert.Multiple(() =>
        {
            // Without the revert, NextShardIndex would be 1 in memory while disk
            // still points at shard 0, so shard 0 would never be unmarked again
            // on a reactivation - leaving it permanently marked deleted.
            Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Unmark));
            Assert.That(state.State.ShardRetries, Is.EqualTo(1));
        });
    }

    [Test]
    public void The_retry_budget_bump_reverts_its_counter_when_WriteStateAsync_throws()
    {
        // Fail the phase body itself, so the failure reaches the outer handler
        // with the retry budget intact and the *bump's* persist is the one that
        // throws. That is the only way to reach the bump's own revert arm.
        var state = SeededState(SnapshotPhase.Unmark, SnapshotMode.Offline);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);
        grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0")
            .UnmarkDeletedAsync()
            .ThrowsAsync(new InvalidOperationException("simulated shard failure"));

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        // The bump's persist fails and rethrows, so this call does throw: there
        // is no second handler to absorb it.
        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.ProcessNextPhaseAsync());

        // Without the revert, ShardRetries would be 1 in memory while disk holds
        // 0, so the budget would be re-burned on every reactivation and the
        // shard would be abandoned after half as many real attempts.
        Assert.That(state.State.ShardRetries, Is.EqualTo(0));
    }

    [Test]
    public void BeginShadowForward_reverts_the_copy_flip_when_WriteStateAsync_throws()
    {
        var state = SeededState(SnapshotPhase.ShadowBegin, SnapshotMode.Online, nextShardIndex: 3, shardRetries: 2);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.BeginShadowForwardAllShardsAsync());

        // The cross-grain BeginShadowForwardAsync calls are deliberately not
        // reverted - they are idempotent on (operationId, destinationTreeId) -
        // but the local phase flip must be, or the activation would start
        // draining while disk still says shadow-forwarding had not begun.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.ShadowBegin));
            Assert.That(state.State.NextShardIndex, Is.EqualTo(3));
            Assert.That(state.State.ShardRetries, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task BeginShadowForward_still_signals_every_source_shard_before_the_failing_persist()
    {
        // The control for the test above: proves the revert is containing a
        // real, completed fan-out rather than an early exit that never ran.
        var state = SeededState(SnapshotPhase.ShadowBegin, SnapshotMode.Online);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await grain.BeginShadowForwardAllShardsAsync());

        for (int i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}")
                .Received(1).BeginShadowForwardAsync(DestTreeId, "op-phase-write-failure", SourceTreeId);
        }
    }

    [Test]
    public void The_online_bulk_drain_reverts_its_cursor_advance_when_WriteStateAsync_throws()
    {
        var h = CreateCopyingSnapshot(leafCount: 2, leavesPerPass: 8, SnapshotMode.Online);
        h.State.State.ShardRetries = 1;
        h.State.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(
            async () => await h.Grain.DrainAllShardsOnlineAsync());

        // Without the revert, NextShardIndex would be parked at ShardCount in
        // memory while disk still holds 0, so RunSnapshotPassAsync would skip
        // straight to completion and declare a snapshot done whose shards were
        // never durably recorded as drained.
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(0));
            Assert.That(h.State.State.ShardRetries, Is.EqualTo(1));
        });
    }
}
