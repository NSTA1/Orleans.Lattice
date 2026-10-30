using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TxRegistryGrainTests
{
    [TestCase("InFlight", false, false)]
    [TestCase("InFlight", true, true)]
    [TestCase("Committed", true, false)]
    [TestCase("Aborted", true, false)]
    [TestCase("Indeterminate", true, true)]
    public async Task HasUndecidedParticipants_matches_the_recorded_status_and_participant_predicate(
        string statusName, bool participants, bool expected)
    {
        var status = Enum.Parse<TxStatus>(statusName);
        var txid = Guid.NewGuid();
        var state = new FakePersistentState<TxRegistryState>();
        if (status != TxStatus.InFlight) state.State.Decisions[txid] = status;
        if (participants) state.State.Participants[txid] = [3, 1];
        var (grain, _) = CreateGrain(state);

        Assert.That((await grain.HasUndecidedParticipantsManyAsync([txid]))[txid], Is.EqualTo(expected));
        Assert.That(state.WriteCount, Is.Zero, "The combined probe is observational.");
    }

    [Test]
    public async Task HasUndecidedParticipants_ignores_an_empty_participant_set()
    {
        var txid = Guid.NewGuid();
        var (grain, state) = CreateGrain();
        state.State.Participants[txid] = [];

        Assert.That((await grain.HasUndecidedParticipantsManyAsync([txid]))[txid], Is.False);
        Assert.That(state.WriteCount, Is.Zero);
    }

    [Test]
    public async Task HasUndecidedParticipants_reads_the_physical_decision_without_a_retention_mask()
    {
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 3, 1, 0, 0, 0, TimeSpan.Zero));
        var (grain, state) = CreateGrain(retention: TimeSpan.FromMinutes(1), timeProvider: clock);
        var txid = Guid.NewGuid();
        state.State.Decisions[txid] = TxStatus.Committed;
        state.State.ForgottenAt[txid] = clock.GetUtcNow() - TimeSpan.FromMinutes(5);
        state.State.Participants[txid] = [1];

        Assert.That(await grain.GetStatusAsync(txid), Is.EqualTo(TxStatus.Indeterminate));
        Assert.That((await grain.HasUndecidedParticipantsManyAsync([txid]))[txid], Is.False);
        Assert.That(state.WriteCount, Is.Zero);
        Assert.That(state.State.ForgottenAt.ContainsKey(txid), Is.True);
    }

    [Test]
    public async Task HasUndecidedParticipants_does_not_resolve_the_delegated_visibility_decision()
    {
        var txid = Guid.NewGuid();
        var (grain, _, coordinator) = CreateGrainWithCoordinator("reseed-probe");
        await grain.RegisterParticipantsAsync(txid, [1]);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "reseed-probe");
        coordinator.ClearReceivedCalls();

        Assert.That((await grain.HasUndecidedParticipantsManyAsync([txid]))[txid], Is.True);
        await coordinator.DidNotReceive().GetDecisionAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task HasUndecidedParticipants_waits_for_a_commit_and_recomputes_after_failure(bool fail)
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (grain, state) = CreateGrain();
        var txid = Guid.NewGuid();
        await grain.RegisterParticipantsAsync(txid, [1]);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        state.BeforeWrite = () => gate.Task;
        if (fail) state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        var mark = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var read = OnTurnAsync(turn, async () => (await grain.HasUndecidedParticipantsManyAsync([txid]))[txid]);
        await DrainAsync(turn);
        try
        {
            Assert.That(read.IsCompleted, Is.False, "An uncommitted verdict cannot release a clamp.");
            Assert.That(await OnTurnAsync(turn, async () => { var unrelated = Guid.NewGuid(); return (await grain.HasUndecidedParticipantsManyAsync([unrelated]))[unrelated]; }), Is.False,
                "An unrelated read must not wait for this transaction's write.");
        }
        finally
        {
            gate.TrySetResult();
        }

        if (fail)
            Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await mark);
        else
            await mark;
        Assert.That(await read, Is.EqualTo(fail));
    }

    [Test]
    public async Task HasUndecidedParticipants_waits_for_participant_registration()
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState();
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();
        var registration = OnTurnAsync(turn, () => grain.RegisterParticipantsAsync(txid, [1]));
        var read = OnTurnAsync(turn, async () => (await grain.HasUndecidedParticipantsManyAsync([txid]))[txid]);
        await DrainAsync(turn);
        try
        {
            Assert.That(read.IsCompleted, Is.False);
        }
        finally
        {
            gate.TrySetResult();
        }

        await registration;
        Assert.That(await read, Is.True);
    }
}
