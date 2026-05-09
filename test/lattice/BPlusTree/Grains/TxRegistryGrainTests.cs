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
}
