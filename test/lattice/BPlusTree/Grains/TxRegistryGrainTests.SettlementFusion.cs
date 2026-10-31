using NSubstitute;
using Orleans.Concurrency;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class TxRegistryGrainTests
{
    [Test]
    public async Task Settlement_batch_matches_scalar_answers_for_live_masked_and_missing_transactions()
    {
        var clock = new Orleans.Lattice.Testing.ManualTimeProvider(DateTimeOffset.UtcNow);
        var (grain, _) = CreateGrain(retention: TimeSpan.FromSeconds(30), timeProvider: clock);
        var committed = Guid.NewGuid();
        var aborted = Guid.NewGuid();
        var live = Guid.NewGuid();
        var missing = Guid.NewGuid();
        await grain.MarkCommittedAsync(committed);
        await grain.MarkAbortedAsync(aborted);
        await grain.MarkCommittedAsync(live);
        await grain.ForgetAsync(committed);
        await grain.ForgetAsync(aborted);
        clock.Advance(TimeSpan.FromSeconds(31));
        Guid[] ids = [committed, aborted, live, missing];

        var batch = await grain.GetStatusManyForSettlementAsync(ids);

        foreach (var id in ids)
            Assert.That(batch[id], Is.EqualTo(await grain.GetStatusForSettlementAsync(id)));
        Assert.That(batch.Values, Is.EquivalentTo(new[] { TxStatus.Committed, TxStatus.Aborted, TxStatus.Committed, TxStatus.InFlight }));
        Assert.That(await grain.GetStatusManyForSettlementAsync([]), Is.Empty);
        Assert.ThrowsAsync<ArgumentNullException>(async () => await grain.GetStatusManyForSettlementAsync(null!));
    }

    [Test]
    public async Task Settlement_batch_keeps_delegated_decisions_behind_the_capture_gate()
    {
        var txid = Guid.NewGuid();
        var (grain, state, coordinator) = CreateGrainWithCoordinator("batch-gated");
        coordinator.GetDecisionAsync().Returns(TxStatus.Committed);
        await grain.RegisterExternalDecisionAuthorityAsync(txid, "batch-gated");
        var token = Guid.NewGuid();
        await grain.AcquireCaptureGateAsync(token, TxRegistryCaptureGateMode.Gate, GateLease);

        Assert.That((await grain.GetStatusManyForSettlementAsync([txid]))[txid], Is.EqualTo(TxStatus.InFlight));
        await coordinator.DidNotReceive().GetDecisionAsync();
        Assert.That(state.State.Decisions.ContainsKey(txid), Is.False);
        await grain.ReleaseCaptureGateAsync(token);
        Assert.That((await grain.GetStatusManyForSettlementAsync([txid]))[txid], Is.EqualTo(TxStatus.Committed));
        Assert.That(state.State.Decisions[txid], Is.EqualTo(TxStatus.Committed));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Settlement_batch_waits_for_durability_and_recomputes_after_rollback(bool failWrite)
    {
        var turn = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
        var (state, gate) = GatedState(failWrite ? new InvalidOperationException("storage unavailable") : null);
        var (grain, _) = CreateGrain(state);
        var txid = Guid.NewGuid();
        var mark = OnTurnAsync(turn, () => grain.MarkCommittedAsync(txid));
        var read = OnTurnAsync(turn, () => grain.GetStatusManyForSettlementAsync([txid]));
        await DrainAsync(turn);
        Assert.That(read.IsCompleted, Is.False);
        gate.SetResult();
        if (failWrite)
            Assert.ThrowsAsync<TxRegistryWriteFailedException>(async () => await mark);
        else
            await mark;
        Assert.That((await read)[txid], Is.EqualTo(failWrite ? TxStatus.InFlight : TxStatus.Committed));
    }

    [Test]
    public async Task Prepare_forgetting_probe_distinguishes_live_forgotten_and_decided_sagas()
    {
        var (grain, _) = CreateGrain();
        var live = Guid.NewGuid();
        await grain.RegisterParticipantAsync(live, 0);
        Assert.That(await grain.IsDecidedOrForgottenForPrepareAsync(live), Is.False);
        await grain.MarkCommittedAsync(live);
        Assert.That(await grain.IsDecidedOrForgottenForPrepareAsync(live), Is.True);
        await grain.ForgetAsync(live);
        Assert.That(await grain.IsDecidedOrForgottenForPrepareAsync(live), Is.True);
    }

    [Test]
    public void Settlement_fused_dispatches_retain_always_interleave()
    {
        foreach (var name in new[] { nameof(ITxRegistryGrain.GetStatusManyForSettlementAsync), nameof(ITxRegistryGrain.IsDecidedOrForgottenForPrepareAsync) })
        {
            var method = typeof(ITxRegistryGrain).GetMethod(name);
            Assert.That(method, Is.Not.Null);
            Assert.That(method!.IsDefined(typeof(AlwaysInterleaveAttribute), false), Is.True);
        }
    }
}
