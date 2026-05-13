using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the Class B "persisted / in-memory divergence on
/// write failure (idempotency-guarded)" anti-pattern on
/// <see cref="TreeDeletionGrain"/>. Each test arranges the grain, forces
/// <see cref="FakePersistentState{T}.ThrowOnWrite"/>, asserts the failing
/// call rethrows, then asserts every mutated field on <c>state.State</c>
/// matches its pre-call snapshot - i.e. the in-memory activation no longer
/// diverges from what storage and any future reactivation observe.
/// </summary>
public partial class TreeDeletionGrainTests
{
    [Test]
    public void DeleteTree_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state, _, _, _) = CreateGrain();

        // Pre-conditions: a freshly-constructed deletion grain is not deleted.
        Assume.That(state.State.IsDeleted, Is.False);
        Assume.That(state.State.DeletedAtUtc, Is.Null);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.DeleteTreeAsync());

        // Without the snapshot-and-restore fix, IsDeleted and DeletedAtUtc
        // remain at their post-mutation values, permanently diverging this
        // activation from storage: the method's idempotency guard
        // `if (state.State.IsDeleted) return;` short-circuits every retry,
        // turning a transient storage failure into a permanent split-brain.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.IsDeleted, Is.False);
            Assert.That(state.State.DeletedAtUtc, Is.Null);
        });
    }

    [Test]
    public async Task Recover_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange: first put the grain into a deleted state via a successful call.
        var (grain, state, _, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        Assume.That(state.State.IsDeleted, Is.True);
        Assume.That(state.State.DeletedAtUtc, Is.Not.Null);

        var preDeletedAtUtc = state.State.DeletedAtUtc;

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.RecoverAsync());

        // Without the fix, the pre-conditions `if (!state.State.IsDeleted) throw`
        // on the next retry would falsely fire because in-memory IsDeleted
        // was cleared while persisted IsDeleted remained true.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.IsDeleted, Is.True);
            Assert.That(state.State.DeletedAtUtc, Is.EqualTo(preDeletedAtUtc));
        });
    }

    [Test]
    public async Task PurgeNow_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange: put the grain in a deleted (but not-yet-purged) state.
        var (grain, state, _, _, _) = CreateGrain();
        await grain.DeleteTreeAsync();

        Assume.That(state.State.IsDeleted, Is.True);
        Assume.That(state.State.PurgeComplete, Is.False);
        Assume.That(state.State.PurgeInProgress, Is.False);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.PurgeNowAsync());

        // Without the fix, PurgeComplete remains true in-memory after the
        // failure. The pre-conditions on retry then trip
        // `if (state.State.PurgeComplete) throw InvalidOperationException`,
        // permanently blocking purge attempts from this activation even
        // though the underlying storage state was never updated.
        Assert.Multiple(() =>
        {
            Assert.That(state.State.PurgeComplete, Is.False);
            Assert.That(state.State.PurgeInProgress, Is.False);
            Assert.That(state.State.NextShardIndex, Is.EqualTo(0));
            Assert.That(state.State.ShardRetries, Is.EqualTo(0));
        });
    }
}
