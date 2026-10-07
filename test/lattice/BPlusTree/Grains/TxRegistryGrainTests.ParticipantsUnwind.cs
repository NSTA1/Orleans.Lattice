using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4533: a replication shipper proves a saga forgotten from the absence
/// of its participant row, so a failed registration must never remove a row a
/// landed prepare depends on. A shard registers before it appends its prepare,
/// and the registry's rollback restores exactly the last durable state.
/// </summary>
public partial class TxRegistryGrainTests
{
    [Test]
    public async Task A_later_shards_failed_registration_keeps_the_landed_shards_participant_row()
    {
        var state = new FakePersistentState<TxRegistryState>();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Shard 0 registered durably, so its prepare may have landed.
        await grain.RegisterParticipantAsync(txid, 0);
        state.ThrowOnWrite = new InvalidOperationException("storage down");

        Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await grain.RegisterParticipantAsync(txid, 1),
            "shard 1's registration fails, so shard 1 never dispatches its prepare");
        state.ThrowOnWrite = null;

        Assert.That(await grain.GetParticipantsAsync(txid), Is.EquivalentTo(new[] { 0 }),
            "the failed registration removes only its own shard; the row shard 0's prepare depends on survives");
    }

    [Test]
    public async Task A_failed_first_registration_fails_every_queued_registration_and_leaves_no_row()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState(new InvalidOperationException("storage down"));
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        // Shard 0's registration creates the row and is in flight; shard 1's
        // joins the row behind it.
        var first = OnTurnAsync(turn, () => grain.RegisterParticipantAsync(txid, 0));
        var queued = OnTurnAsync(turn, () => grain.RegisterParticipantAsync(txid, 1));
        await DrainAsync(turn);
        gate.SetResult();

        Assert.Multiple(() =>
        {
            Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await first, "shard 0 never dispatches its prepare");
            Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await queued,
                "shard 1 rode on state that never became durable, so it fails too and never dispatches its prepare");
            Assert.That(state.State.Participants.ContainsKey(txid), Is.False,
                "no row is left, and no prepare of the saga was dispatched");
        });
    }
}
