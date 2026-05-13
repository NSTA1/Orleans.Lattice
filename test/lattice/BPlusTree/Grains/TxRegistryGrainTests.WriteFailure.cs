using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

// Class B regression tests: every mutating method on TxRegistryGrain
// assigns to state.State BEFORE awaiting WriteStateAsync, and every
// method short-circuits on its post-mutation in-memory observation
// (Decisions[txid] == outcome or set.Contains(shardIndex)). A failing
// WriteStateAsync therefore leaves an in-memory decision/participant
// that disk does not have, and a subsequent retry from the same
// activation no-ops the short-circuit instead of re-persisting.
public partial class TxRegistryGrainTests
{
    [Test]
    public void MarkCommittedAsync_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var state = new FakePersistentState<TxRegistryState>
        {
            ThrowOnWrite = new InvalidOperationException("write boom"),
        };
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        Assert.That(async () => await grain.MarkCommittedAsync(txid),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.Decisions.ContainsKey(txid), Is.False,
            "in-memory Decisions must not retain the entry when the persist failed");
    }

    [Test]
    public void MarkAbortedAsync_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var state = new FakePersistentState<TxRegistryState>
        {
            ThrowOnWrite = new InvalidOperationException("write boom"),
        };
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();

        Assert.That(async () => await grain.MarkAbortedAsync(txid),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        Assert.That(state.State.Decisions.ContainsKey(txid), Is.False,
            "in-memory Decisions must not retain the entry when the persist failed");
    }

    [Test]
    public async Task ForgetAsync_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        // Seed the registry with a committed decision and a participant
        // entry so ForgetAsync has something to remove on the failing call.
        await grain.MarkCommittedAsync(txid);
        await grain.RegisterParticipantAsync(txid, shardIndex: 3);
        Assert.That(state.State.Decisions.ContainsKey(txid), Is.True);
        Assert.That(state.State.Participants.ContainsKey(txid), Is.True);

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.ForgetAsync(txid),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        // After the failing write, the in-memory view must still observe
        // the saga - otherwise a subsequent GetStatusAsync from the same
        // activation returns InFlight while disk still has the decision.
        Assert.That(state.State.Decisions.ContainsKey(txid), Is.True,
            "in-memory Decisions must retain the entry when the persist failed");
        Assert.That(state.State.Participants.ContainsKey(txid), Is.True,
            "in-memory Participants must retain the entry when the persist failed");
    }

    [Test]
    public void RegisterParticipantAsync_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterParticipantAsync(txid, shardIndex: 7),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("write boom"));

        // After the failing write, in-memory Participants must not retain
        // the shard - otherwise a retry from the same activation finds
        // the shard already present, short-circuits before WriteStateAsync,
        // and the participant is never persisted.
        if (state.State.Participants.TryGetValue(txid, out var set))
        {
            Assert.That(set.Contains(7), Is.False,
                "in-memory Participants must not retain the shard index when the persist failed");
        }
    }

    [Test]
    public async Task RegisterParticipantAsync_does_not_short_circuit_on_retry_after_failed_persist()
    {
        // Tightens the loop closed by the previous test: the *intended*
        // observable is that a caller retrying after a failed persist
        // sees the second call actually persist. Without the revert, the
        // retry hits the !set.Add(shardIndex) short-circuit and silently
        // no-ops.
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        state.ThrowOnWrite = new InvalidOperationException("write boom");

        Assert.That(async () => await grain.RegisterParticipantAsync(txid, shardIndex: 7),
            Throws.TypeOf<InvalidOperationException>());

        // ThrowOnWrite is one-shot; the retry must succeed and persist.
        await grain.RegisterParticipantAsync(txid, shardIndex: 7);

        Assert.That(state.WriteCount, Is.EqualTo(1),
            "the retry must reach WriteStateAsync exactly once");
        Assert.That(state.State.Participants[txid].Contains(7), Is.True,
            "the retry must record the participant in persisted state");
    }
}
