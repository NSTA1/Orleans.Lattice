using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TxRegistryGrainTests
{
    [TestCase("InFlight", false, true)]
    [TestCase("InFlight", true, false)]
    [TestCase("Committed", false, false)]
    [TestCase("Aborted", false, false)]
    [TestCase("Indeterminate", false, true)]
    [TestCase("Indeterminate", true, false)]
    public async Task Maintenance_probes_match_physical_rows(string statusName, bool participants, bool purged)
    {
        var txid = Guid.NewGuid();
        var state = new FakePersistentState<TxRegistryState>();
        var status = Enum.Parse<TxStatus>(statusName);
        if (status != TxStatus.InFlight) state.State.Decisions[txid] = status;
        if (participants) state.State.Participants[txid] = [1];
        var (grain, _) = CreateGrain(state);

        Assert.That(await grain.IsPurgedAsync(txid), Is.EqualTo(purged));
        var batch = await grain.HasUndecidedParticipantsManyAsync([txid, txid, Guid.Empty]);
        Assert.That(batch[txid], Is.EqualTo(participants && status is not (TxStatus.Committed or TxStatus.Aborted)));
        Assert.That(batch[Guid.Empty], Is.False);
        Assert.That(batch.Count, Is.EqualTo(2));
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task Maintenance_probes_ignore_retention_masks()
    {
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        state.State.Decisions[txid] = TxStatus.Committed;
        state.State.ForgottenAt[txid] = DateTimeOffset.MinValue;
        state.State.Participants[txid] = [1];

        Assert.That(await grain.IsPurgedAsync(txid), Is.False);
        Assert.That((await grain.HasUndecidedParticipantsManyAsync([txid]))[txid], Is.False);
        Assert.That(state.WriteCount, Is.Zero);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Maintenance_probes_never_resolve_delegated_visibility(bool receiver)
    {
        var factory = Substitute.For<IGrainFactory>();
        var (grain, state) = CreateGrain(grainFactory: factory);
        var txid = Guid.NewGuid();
        var authorities = receiver ? state.State.ReceiverDecisionAuthorities : state.State.ExternalAuthorities;
        authorities[txid] = "delegated";
        state.State.Participants[txid] = [1];

        Assert.That(await grain.IsPurgedAsync(txid), Is.False);
        Assert.That((await grain.HasUndecidedParticipantsManyAsync([txid]))[txid], Is.True);
        Assert.That(factory.ReceivedCalls(), Is.Empty);
        Assert.That(state.State.Decisions, Is.Empty);
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task Maintenance_batch_validates_its_bound_and_handles_empty_input()
    {
        var (grain, _) = CreateGrain();
        Assert.That(await grain.HasUndecidedParticipantsManyAsync([]), Is.Empty);
        Assert.ThrowsAsync<ArgumentNullException>(() => grain.HasUndecidedParticipantsManyAsync(null!));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            grain.HasUndecidedParticipantsManyAsync(new Guid[ITxRegistryGrain.MaxMaintenanceProbeBatchSize + 1]));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Maintenance_probes_wait_for_registration_and_recompute_on_rollback(bool fail)
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();
        if (fail) state.ThrowOnWrite = new InvalidOperationException("storage unavailable");
        var registration = OnTurnAsync(turn, () => grain.RegisterParticipantsAsync(txid, [1]));
        var purge = OnTurnAsync(turn, () => grain.IsPurgedAsync(txid));
        var batch = OnTurnAsync(turn, () => grain.HasUndecidedParticipantsManyAsync([Guid.Empty, txid]));
        await DrainAsync(turn);
        try
        {
            Assert.That(purge.IsCompleted, Is.False);
            Assert.That(batch.IsCompleted, Is.False);
            Assert.That(await OnTurnAsync(turn, () => grain.IsPurgedAsync(Guid.NewGuid())), Is.True);
        }
        finally
        {
            gate.TrySetResult();
        }
        if (fail)
            Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await registration);
        else
            await registration;
        Assert.That(await purge, Is.EqualTo(fail));
        Assert.That((await batch)[txid], Is.EqualTo(!fail));
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Maintenance_probes_wait_for_decisions_and_recompute_on_rollback(bool fail, bool participants)
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();
        if (participants) state.State.Participants[txid] = [1];
        if (fail) state.ThrowOnWrite = new InvalidOperationException("storage unavailable");
        var decision = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var purge = OnTurnAsync(turn, () => grain.IsPurgedAsync(txid));
        var batch = OnTurnAsync(turn, () => grain.HasUndecidedParticipantsManyAsync([txid]));
        await DrainAsync(turn);
        try
        {
            Assert.That(purge.IsCompleted, Is.False);
            Assert.That(batch.IsCompleted, Is.False);
        }
        finally
        {
            gate.TrySetResult();
        }
        if (fail)
            Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await decision);
        else
            await decision;
        Assert.That(await purge, Is.EqualTo(fail && !participants));
        Assert.That((await batch)[txid], Is.EqualTo(fail && participants));
    }
}
