using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class TxRegistryGrainTests
{
    private static (TxRegistryGrain grain, FakePersistentState<TxRegistryState> state) CreateGrain(
        FakePersistentState<TxRegistryState>? state = null,
        string treeId = "tree-x")
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("tx-registry", treeId));
        state ??= new FakePersistentState<TxRegistryState>();
        return (new TxRegistryGrain(context, state), state);
    }

    [Test]
    public async Task GetStatusAsync_returns_InFlight_for_unknown_txid()
    {
        var (grain, _) = CreateGrain();

        var status = await grain.GetStatusAsync(Guid.NewGuid());

        Assert.That(status, Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task GetStatusAsync_returns_Committed_after_MarkCommitted()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);
        var status = await grain.GetStatusAsync(txid);

        Assert.That(status, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task GetStatusAsync_returns_Aborted_after_MarkAborted()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkAbortedAsync(txid);
        var status = await grain.GetStatusAsync(txid);

        Assert.That(status, Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task MarkCommittedAsync_persists_decision_to_state()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.Decisions.TryGetValue(txid, out var status), Is.True);
        Assert.That(status, Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task MarkAbortedAsync_persists_decision_to_state()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkAbortedAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.Decisions.TryGetValue(txid, out var status), Is.True);
        Assert.That(status, Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task MarkCommittedAsync_is_idempotent_under_repeated_calls()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);
        await grain.MarkCommittedAsync(txid);
        await grain.MarkCommittedAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(1),
            "Re-marking with the same outcome must short-circuit before persisting.");
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task MarkAbortedAsync_is_idempotent_under_repeated_calls()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkAbortedAsync(txid);
        await grain.MarkAbortedAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public void MarkCommittedAsync_throws_when_previously_aborted()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        Assert.That(async () =>
        {
            await grain.MarkAbortedAsync(txid);
            await grain.MarkCommittedAsync(txid);
        }, Throws.InvalidOperationException);
    }

    [Test]
    public void MarkAbortedAsync_throws_when_previously_committed()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        Assert.That(async () =>
        {
            await grain.MarkCommittedAsync(txid);
            await grain.MarkAbortedAsync(txid);
        }, Throws.InvalidOperationException);
    }

    [Test]
    public async Task GetStatusManyAsync_returns_status_for_each_requested_txid()
    {
        var (grain, _) = CreateGrain();
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        var unknown = Guid.NewGuid();
        await grain.MarkCommittedAsync(committed);
        await grain.MarkAbortedAsync(aborted);

        var result = await grain.GetStatusManyAsync(new[] { committed, aborted, unknown });

        Assert.Multiple(() =>
        {
            Assert.That(result, Has.Count.EqualTo(3));
            Assert.That(result[committed], Is.EqualTo(TxStatus.Committed));
            Assert.That(result[aborted], Is.EqualTo(TxStatus.Aborted));
            Assert.That(result[unknown], Is.EqualTo(TxStatus.InFlight));
        });
    }

    [Test]
    public async Task GetStatusManyAsync_returns_empty_map_for_empty_input()
    {
        var (grain, _) = CreateGrain();

        var result = await grain.GetStatusManyAsync(Array.Empty<Guid>());

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void GetStatusManyAsync_throws_on_null_input()
    {
        var (grain, _) = CreateGrain();

        Assert.That(async () => await grain.GetStatusManyAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task SnapshotAsync_returns_empty_map_when_registry_empty()
    {
        var (grain, _) = CreateGrain();

        var snapshot = await grain.SnapshotAsync();

        Assert.That(snapshot, Is.Empty);
    }

    [Test]
    public async Task SnapshotAsync_returns_all_recorded_decisions()
    {
        var (grain, _) = CreateGrain();
        var c1 = Guid.NewGuid();
        var c2 = Guid.NewGuid();
        var a1 = Guid.NewGuid();
        await grain.MarkCommittedAsync(c1);
        await grain.MarkCommittedAsync(c2);
        await grain.MarkAbortedAsync(a1);

        var snapshot = await grain.SnapshotAsync();

        Assert.Multiple(() =>
        {
            Assert.That(snapshot, Has.Count.EqualTo(3));
            Assert.That(snapshot[c1], Is.EqualTo(TxStatus.Committed));
            Assert.That(snapshot[c2], Is.EqualTo(TxStatus.Committed));
            Assert.That(snapshot[a1], Is.EqualTo(TxStatus.Aborted));
        });
    }

    [Test]
    public async Task SnapshotAsync_returns_defensive_copy_isolated_from_state()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        var snapshot = await grain.SnapshotAsync();
        snapshot[Guid.NewGuid()] = TxStatus.Aborted;
        snapshot.Remove(txid);

        Assert.That(state.State.Decisions, Has.Count.EqualTo(1),
            "Mutating the snapshot must not bleed back into the registry's persisted state.");
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task ForgetAsync_drops_recorded_decision()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);

        Assert.That(state.State.Decisions, Does.Not.ContainKey(txid));
        var status = await grain.GetStatusAsync(txid);
        Assert.That(status, Is.EqualTo(TxStatus.InFlight));
    }

    [Test]
    public async Task ForgetAsync_persists_when_decision_was_present()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);
        var initialWrites = state.WriteCount;

        await grain.ForgetAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(initialWrites + 1));
    }

    [Test]
    public async Task ForgetAsync_is_noop_when_decision_absent()
    {
        var (grain, state) = CreateGrain();

        await grain.ForgetAsync(Guid.NewGuid());

        Assert.That(state.WriteCount, Is.EqualTo(0),
            "Forgetting an unknown txid must not trigger a state write.");
    }

    [Test]
    public async Task ForgetAsync_is_idempotent_under_repeated_calls()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);
        await grain.ForgetAsync(txid);
        await grain.ForgetAsync(txid);

        // First Mark = 1 write, first Forget = 1 write, subsequent Forgets = 0.
        Assert.That(state.WriteCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Mark_then_Forget_then_Mark_records_fresh_decision()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.MarkCommittedAsync(txid);
        await grain.ForgetAsync(txid);
        // After forgetting, recording the opposite outcome is allowed -
        // the conflict-detection guard only fires while a prior decision
        // remains in the map.
        await grain.MarkAbortedAsync(txid);

        var status = await grain.GetStatusAsync(txid);
        Assert.That(status, Is.EqualTo(TxStatus.Aborted));
    }

    [Test]
    public async Task Multiple_distinct_txids_are_recorded_independently()
    {
        var (grain, state) = CreateGrain();
        var ids = Enumerable.Range(0, 16).Select(_ => Guid.NewGuid()).ToArray();

        foreach (var id in ids)
            await grain.MarkCommittedAsync(id);

        Assert.That(state.State.Decisions, Has.Count.EqualTo(ids.Length));
        foreach (var id in ids)
            Assert.That(state.State.Decisions[id], Is.EqualTo(TxStatus.Committed));
    }

    [Test]
    public async Task GetParticipantsAsync_returns_empty_for_unknown_txid()
    {
        var (grain, _) = CreateGrain();

        var participants = await grain.GetParticipantsAsync(Guid.NewGuid());

        Assert.That(participants, Is.Empty);
    }

    [Test]
    public async Task RegisterParticipantAsync_records_single_shard()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.RegisterParticipantAsync(txid, 3);

        Assert.That(state.WriteCount, Is.EqualTo(1));
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.EquivalentTo(new[] { 3 }));
    }

    [Test]
    public async Task RegisterParticipantAsync_records_multiple_shards_for_same_txid()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.RegisterParticipantAsync(txid, 0);
        await grain.RegisterParticipantAsync(txid, 5);
        await grain.RegisterParticipantAsync(txid, 2);

        Assert.That(state.WriteCount, Is.EqualTo(3));
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.EqualTo(new[] { 0, 2, 5 }),
            "Participants must be returned sorted ascending so the saga's broadcast iteration is deterministic.");
    }

    [Test]
    public async Task RegisterParticipantAsync_is_idempotent_on_repeated_pair()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();

        await grain.RegisterParticipantAsync(txid, 2);
        await grain.RegisterParticipantAsync(txid, 2);
        await grain.RegisterParticipantAsync(txid, 2);

        Assert.That(state.WriteCount, Is.EqualTo(1),
            "Re-registering the same shard for the same txid must short-circuit before persisting.");
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.EquivalentTo(new[] { 2 }));
    }

    [Test]
    public async Task RegisterParticipantAsync_isolates_participants_across_txids()
    {
        var (grain, _) = CreateGrain();
        var tx1 = Guid.NewGuid();
        var tx2 = Guid.NewGuid();

        await grain.RegisterParticipantAsync(tx1, 0);
        await grain.RegisterParticipantAsync(tx1, 1);
        await grain.RegisterParticipantAsync(tx2, 2);
        await grain.RegisterParticipantAsync(tx2, 3);

        var p1 = await grain.GetParticipantsAsync(tx1);
        var p2 = await grain.GetParticipantsAsync(tx2);

        Assert.Multiple(() =>
        {
            Assert.That(p1, Is.EqualTo(new[] { 0, 1 }));
            Assert.That(p2, Is.EqualTo(new[] { 2, 3 }));
        });
    }

    [Test]
    public async Task ForgetAsync_drops_participants_alongside_decision()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, 0);
        await grain.RegisterParticipantAsync(txid, 1);
        await grain.MarkCommittedAsync(txid);

        await grain.ForgetAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Decisions, Does.Not.ContainKey(txid));
            Assert.That(state.State.Participants, Does.Not.ContainKey(txid));
        });
        var participants = await grain.GetParticipantsAsync(txid);
        Assert.That(participants, Is.Empty);
    }

    [Test]
    public async Task ForgetAsync_persists_when_only_participants_present()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, 0);
        var initialWrites = state.WriteCount;

        await grain.ForgetAsync(txid);

        Assert.That(state.WriteCount, Is.EqualTo(initialWrites + 1),
            "Forgetting a txid that has only participants (no decision) must still drop the participants and persist.");
        Assert.That(state.State.Participants, Does.Not.ContainKey(txid));
    }

    [Test]
    public async Task GetParticipantsAsync_returns_independent_snapshot()
    {
        var (grain, _) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantAsync(txid, 0);
        await grain.RegisterParticipantAsync(txid, 1);

        var first = await grain.GetParticipantsAsync(txid);
        await grain.RegisterParticipantAsync(txid, 2);
        var second = await grain.GetParticipantsAsync(txid);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(new[] { 0, 1 }),
                "First snapshot must reflect the registry state at the moment of the call, not be aliased to live state.");
            Assert.That(second, Is.EqualTo(new[] { 0, 1, 2 }));
        });
    }
}
