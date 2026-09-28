using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the coordinator's per-tree key-distinctness check. Every
/// participant sub-saga rejects a slice that repeats a key, deterministically,
/// so the coordinator must reject it at admission - before it persists
/// <see cref="CrossTreeTxPhase.Preparing"/>, arms its keepalive, or parks any
/// other tree's sub-saga - or the transaction can never reach a decision.
/// </summary>
public partial class LatticeCrossTreeTxGrainTests
{
    [Test]
    public async Task CommitAsync_duplicate_key_within_a_tree_throws_before_anything_is_staged()
    {
        // A repeated key inside one tree's slice is rejected deterministically by
        // that tree's sub-saga, so a coordinator that admitted it would persist
        // Preparing, arm its keepalive, park the other trees' sub-sagas, and then
        // re-dispatch a prepare that can never succeed on every keepalive tick.
        var (grain, state, _, participants) = CreateGrain(["orders", "inventory"]);
        var reminderRegistry = ExtractReminderRegistry(grain);
        var batches = new List<LatticeTreeBatch>
        {
            new("orders", [new("order:1", [1]), new("order:1", [2])]),
            new("inventory", [new("sku:1", [3])]),
        };

        var ex = Assert.ThrowsAsync<ArgumentException>(() => grain.CommitAsync(batches));

        Assert.That(ex!.Message, Does.Contain("duplicate key 'order:1'"));
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.NotStarted));
            Assert.That(state.State.Participants, Is.Empty);
            Assert.That(state.State.Fingerprint, Is.Null);
        });
        foreach (var participant in participants.Values)
        {
            await participant.DidNotReceiveWithAnyArgs().PrepareForCoordinatorAsync(
                default!, default!, default, default!, default!, default, default);
        }
        await reminderRegistry.DidNotReceiveWithAnyArgs().RegisterOrUpdateReminder(
            default, default!, default, default);
    }

    [Test]
    public void CommitAsync_set_and_delete_of_the_same_key_within_a_tree_throws()
    {
        // The per-entry delete channel does not make a repeated key distinct: a
        // Set and a Delete of one key in one slice is the same duplicate.
        var (grain, state, _, _) = CreateGrain(["orders"]);
        var batches = new List<LatticeTreeBatch>
        {
            new("orders", [new("order:1", [1]), new("order:1", [])], EntryDeletes: [false, true]),
        };

        Assert.ThrowsAsync<ArgumentException>(() => grain.CommitAsync(batches));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.NotStarted));
    }

    [Test]
    public async Task CommitAsync_same_key_in_different_trees_is_not_a_duplicate()
    {
        // Distinctness is per tree: the same key in two different trees is two
        // different writes and must commit normally.
        var (grain, _, _, _) = CreateGrain(["orders", "inventory"]);

        var outcome = await grain.CommitAsync(Batches(
            ("orders", "shared", "A"),
            ("inventory", "shared", "B")));

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
    }

    [Test]
    public async Task CommitAsync_retry_after_a_rejected_duplicate_key_batch_commits_under_the_same_operation_id()
    {
        // The rejection persists nothing, so the operationId is not burned: a
        // corrected resubmission is a fresh admission, not a key-set mismatch.
        var (grain, state, _, _) = CreateGrain(["orders", "inventory"]);
        var bad = new List<LatticeTreeBatch>
        {
            new("orders", [new("order:1", [1]), new("order:1", [2])]),
            new("inventory", [new("sku:1", [3])]),
        };
        Assert.ThrowsAsync<ArgumentException>(() => grain.CommitAsync(bad));

        var outcome = await grain.CommitAsync(Batches(("orders", "order:1", "A"), ("inventory", "sku:1", "B")));

        Assert.That(outcome, Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(CrossTreeTxPhase.Completed));
    }
}
